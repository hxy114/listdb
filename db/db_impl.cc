// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include <iostream>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <set>
#include <string>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }
  Output* current_outputsub(int k) {  return &outputss[k].back(); }
  void merge(){
    int size=0;
    for(int i=0;i<FIRST_L0_THREAD_NUMBER;i++){
      size+=outputss[i].size();
    }
    outputs.reserve(size);
    for(int i=0;i<FIRST_L0_THREAD_NUMBER;i++){
      outputs.insert(outputs.end(),outputss[i].begin(),outputss[i].end());
    }
    for(int i = 0;i<FIRST_L0_THREAD_NUMBER;i++) {
      for(auto &pair:pages_[i]) {
        page_[pair.first]+=pair.second;
      }
    }
  }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0),
        compactionL0(nullptr){}
  explicit CompactionState(CompactionL0* c)
      : compaction(nullptr),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0),
        compactionL0(c),
        outfiles(FIRST_L0_THREAD_NUMBER, nullptr),
        builders(FIRST_L0_THREAD_NUMBER, nullptr),
        outputss(FIRST_L0_THREAD_NUMBER,std::vector<Output>()),
        pages_(FIRST_L0_THREAD_NUMBER){}

  Compaction* const compaction;
  CompactionL0* const compactionL0;
  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;
  std::vector<std::vector<Output>> outputss;
  // State kept for output being generated
  WritableFile* outfile;
  std::vector<WritableFile*>outfiles;
  TableBuilder* builder;
  std::vector<TableBuilder*>builders;

  uint64_t total_bytes;
  std::map<size_t ,int32_t>page_;
  std::vector<std::map<size_t ,int32_t>>pages_;
  port::Mutex mutex;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  //ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      background_work_finished_signal_imm_(&mutex_),
      background_work_finished_signal_sort_(&mutex_),
      background_work_finished_signal_merge_(&mutex_),
      background_work_finished_signal_L0_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      sort_table_(nullptr),
      merge_imm_use_pages_(0),
      big_table_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)),
      is_first_flush_(true),
      is_start_(true),
      sort_finish_(false),
      merge_finish_(false),
      do_first_compacting_(false){}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  //std::cout<<"xigou kais lock"<<std::endl;
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  //std::cout<<"xigou kais1"<<std::endl;
  while (background_compaction_scheduled_L0_) {
    background_work_finished_signal_L0_.Wait();
  }
  //std::cout<<"xigou kais2"<<std::endl;
  background_work_finished_signal_sort_.SignalAll();
  background_work_finished_signal_merge_.SignalAll();
  mutex_.Unlock();
  while(!sort_finish_.load());
  //std::cout<<"xigou kais3"<<std::endl;
  while(!merge_finish_.load());
  //std::cout<<"xigou kais4"<<std::endl;
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
  //std::cout<<"xigou end"<<std::endl;
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFilesL0() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = (number == logfile_number_);
          /*keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));*/
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        /*Log(options_.info_log, "%d,Delete typeL0=%d #%lld\n", gettid(),static_cast<int>(type),
            static_cast<unsigned long long>(number));*/
        Log(options_.info_log, "Delete typeL0=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
  //Log(options_.info_log,"delete file success lock");
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = (number == logfile_number_);
          /*keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));*/
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
  //Log(options_.info_log,"delete file success lock");
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      nvmManager=new NvmManager(false);
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_, this);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_, this);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
    //Log(options_.info_log, "build lock");
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  /*mutex_.AssertHeld();
  assert(imm_ != nullptr);
  imm_.
  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    //RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }*/
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompactionL0() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_L0_== L0_THREAD_NUMBER) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (background_compaction_scheduled_L0_ >= compaction_tables_.size()) {
    // No work to be done
  } else {
    background_compaction_scheduled_L0_++;
    env_->ScheduleL0(&DBImpl::BGWorkL0, this);
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWorkL0(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallL0();
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCallL0() {
  MutexLock l(&mutex_);
  //Log(options_.info_log, "backgroundL0 lock");
  bool is_comp=false;
  assert(background_compaction_scheduled_L0_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    is_comp=BackgroundCompactionL0();
  }

  background_compaction_scheduled_L0_ --;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.


  if(is_comp){
    MaybeScheduleCompactionL0();
    MaybeScheduleCompaction();
    background_work_finished_signal_merge_.SignalAll();
    background_work_finished_signal_L0_.SignalAll();
  }

}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  //Log(options_.info_log, "background lock");
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

bool DBImpl::BackgroundCompactionL0(){
  mutex_.AssertHeld();
  auto current=versions_->current();
  current->Ref();
  //Log(options_.info_log,"seek L0");
  CompactionL0* c= nullptr;
  for(auto table: compaction_tables_) {
    if(table->GetRole()==MemTable::PEDDINGCOMPACT) {
      table->SetRole(MemTable::COMPACTING);
      std::vector<FileMetaData*>input;
      InternalKey start(table->start_key_, kMaxSequenceNumber, ValueType::kTypeValue), end(table->end_key_, 0, ValueType::kTypeValue);
      current->GetOverlappingInputs(1,&start, &end,&input);
      c = versions_->PickCompactionL0(table,input,&start, &end, current);
      break;
    }
  }


  Status status;
  if (c == nullptr) {
    current->Unref();
    return false;
    // Nothing to do
  }  else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWorkL0(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompactionL0(compact);
    c->ReleaseInputs();
    RemoveObsoleteFilesL0();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction errorL0: %s", status.ToString().c_str());
  }
  return true;
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::GetCompactionRange(std::string &start_key,std::string &end_key, bool &is_start, bool &is_end) {
  auto current = versions_->current();
  //current->Ref();
  //mutex_.Unlock();
  if(is_start_) {
    big_table_->GetFirst(&start_key);
  }
  auto files = current->files_[1];
  int left = 0;
  int right = files.size() - 1;
  int first_index = -1;
  int last_index = -1;


  while (left <= right) {
    int mid = left + (right - left) / 2;

    if (user_comparator()->Compare(files[mid]->smallest.user_key(),start_key) > 0) {
      // 如果找到的文件的 start_key 大于等于 target_key，记录位置并向左查找
      first_index = mid;
      right = mid - 1;
    } else {
      // 否则向右查找
      left = mid + 1;
    }
  }

  // 检查是否需要返回上一个文件的下标
  if (first_index > 0 && user_comparator()->Compare(start_key , files[first_index - 1]->largest.user_key()) <=0) {
    --first_index; // 返回上一个文件的下标
  }
  if (first_index == -1) {
    if (!files.empty() && user_comparator()->Compare(start_key,  files.back()->largest.user_key()) <= 0 ) {
      first_index = files.size() - 1; // target_key 在最后一个文件范围内，返回最后一个文件的下标
    } else {
      first_index = files.size(); // target_key 不在最后一个文件范围内，返回最大下标 + 1
    }
  }
  if(files.size() <20) {
    last_index = first_index +1;
  } else {
    //last_index = first_index + files.size() / 7 ;
    last_index = first_index + (files.size() / 7 >5 ? 5: files.size() / 7);
    if(last_index >= files.size() - 1) {
      is_end = true;
    } else {
      end_key = files[last_index]->largest.user_key().ToString();
    }

  }
  //mutex_.Lock();
 // current->Unref();
  //mutex_.Unlock();

}
bool DBImpl::GenerateCompaction() {
  if(is_first_flush_) {
    Slice start, end;
    bool is_end = true;
    //mutex_.Unlock();
    MemTable * new_table=big_table_->Split(start, end, true, is_end);
   // mutex_.Lock();
    compaction_tables_.push_back(new_table);
    new_table->Ref();
    //background_work_finished_signal_L0_.SignalAll();
    is_first_flush_ = false;
    //mutex_.Unlock();
    MaybeScheduleCompactionL0();
    do_first_compacting_ = true;
    return true;
  } else {
    MemTable * new_table= nullptr;
    bool ret_end = false;
    while(new_table == nullptr) {
      std::string start, end;
      bool is_start, is_end=false;
      is_start = is_start_;
      start = flush_key_;
      GetCompactionRange(start, end,is_start, is_end);
      //mutex_.Unlock();
      Slice sk(start),ek(end);
      new_table=big_table_->Split(sk, ek,is_start,is_end);

      //mutex_.Lock();

      //background_work_finished_signal_L0_.SignalAll();
      //mutex_.Unlock();
      if(is_end) {
        is_start_ = true;
        flush_key_.clear();
        ret_end = true;
      } else {
        is_start_ =false;
        flush_key_ = end;
      }

    }
    if(new_table->is_first_flush_) {
      do_first_compacting_=true;
    }
    compaction_tables_.push_back(new_table);
    new_table->Ref();
    MaybeScheduleCompactionL0();
    return ret_end;
  }

}
void DBImpl::BackGroundTableSort() {

  while(!shutting_down_.load()) {
    mutex_.Lock();
    //Log(options_.info_log, "sort start lock");
    while(sort_table_ == nullptr && !shutting_down_.load()) {
      background_work_finished_signal_sort_.Wait();
    }
    if(!shutting_down_.load()) {
      auto mem =sort_table_;
      mem->SetRole(MemTable::SORTING);
      mem->nvmArena_ = new NvmArena();
      mem->nvmArena_->refs_ = mem->refs_;
      mutex_.Unlock();
      mem->Sort();
      mutex_.Lock();
      //Log(options_.info_log, "sort start2 lock");
      mem->SetRole(MemTable::PEDDINGMERGE);
      merge_imm_.push_back(mem);
      merge_imm_use_pages_ += mem->nvmArena_->pages.size();
      sort_table_= nullptr;
      background_work_finished_signal_imm_.SignalAll();
      background_work_finished_signal_merge_.SignalAll();

    }
    //Log(options_.info_log, "sort finish");
    mutex_.Unlock();

  }
  sort_finish_.store(true, std::memory_order_release);

}
bool DBImpl::HaveCompaction() {
  if(big_table_->status_ == MemTable::READ) {
    return false;
  }
  if(is_first_flush_) {
    if(PAGE_NUMBER - nvmManager->get_free_page_number() - merge_imm_use_pages_ >= 2 * 1024 * 1024 * 1024UL / PAGE_SIZE) {
      return true;
    }
    return false;

  } else {
    if(do_first_compacting_) {
      return false;
    }else if(compaction_tables_.size() < L0_THREAD_NUMBER /*+ 2*/ && PAGE_NUMBER - nvmManager->get_free_page_number() - merge_imm_use_pages_ /*- background_compaction_scheduled_L0_ * 300 */>= 4 * 1024 * 1024 * 1024UL / PAGE_SIZE) {
      return true;
    }
    return false;
  }
}
void DBImpl::BackgroundTableCompaction() {

  while(!shutting_down_.load()) {
    mutex_.Lock();
    //Log(options_.info_log, "compaction start lock");
    while(merge_imm_.empty() && !HaveCompaction() && !shutting_down_.load()) {
      background_work_finished_signal_merge_.Wait();
    }
    if(!merge_imm_.empty()) {
      int n =0, k=2;
      std::vector<MemTable*>tables;

      for(auto table:merge_imm_) {
        tables.push_back(table);
        table->SetRole(MemTable::MERGEING);
        if(++n>=2){
          break;
        }
      }
      mutex_.Unlock();
      big_table_->Compaction(tables,this);

      mutex_.Lock();
      //Log(options_.info_log, "compaction start2 lock");
      for(int i =0;i<tables.size();i++) {
        merge_imm_.pop_front();
        merge_imm_use_pages_ -= tables[i]->nvmArena_->pages.size();
        tables[i]->SetRole(MemTable::MERGED);
        tables[i]->Unref();


      }
    }
    //mutex_.Unlock();

    while(!shutting_down_.load() && HaveCompaction()) {
      bool result=GenerateCompaction();
      if(result) {
        big_table_->ChangeArena();

      }
    }
    //Log(options_.info_log, "compaction finish");
    mutex_.Unlock();
    //TODO 唤醒

    //env_->SleepForMicroseconds(100);

  }

  merge_finish_.store(true, std::memory_order_release);


}

void DBImpl::CleanupCompactionL0(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;

  for(int k=0;k<FIRST_L0_THREAD_NUMBER;k++){
    if (compact->builders[k] != nullptr) {
      // May happen if we get a shutdown call in the middle of compaction
      compact->builders[k]->Abandon();
      delete compact->builders[k];
    } else {
      assert(compact->outfiles[k] == nullptr);
    }
    delete compact->outfiles[k];
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFileSub(CompactionState* compact,int k) {
  assert(compact != nullptr);
  assert(compact->builders[k] == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    //Log(options_.info_log, "open outputsub lock");
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputss[k].push_back(out);

    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfiles[k]);
  if (s.ok()) {
    compact->builders[k] = new TableBuilder(options_, compact->outfiles[k]);
  }
  return s;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    //Log(options_.info_log, "open output lock");
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFileL0(CompactionState* compact,
                                            Iterator* input,int k) {
  assert(compact != nullptr);
  assert(compact->outfiles[k] != nullptr);
  assert(compact->builders[k] != nullptr);

  const uint64_t output_number = compact->current_outputsub(k)->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builders[k]->NumEntries();
  if (s.ok()) {
    s = compact->builders[k]->Finish();
  } else {
    compact->builders[k]->Abandon();
  }
  const uint64_t current_bytes = compact->builders[k]->FileSize();
  compact->current_outputsub(k)->file_size = current_bytes;
  compact->mutex.Lock();
  compact->total_bytes += current_bytes;
  compact->mutex.Unlock();
  delete compact->builders[k];
  compact->builders[k] = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfiles[k]->Sync();
  }
  if (s.ok()) {
    s = compact->outfiles[k]->Close();
  }
  delete compact->outfiles[k];
  compact->outfiles[k] = nullptr;
  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      /*Log(options_.info_log, "%d Generated tableL0 #%llu@%d: %lld keys, %lld bytes",
          gettid(),(unsigned long long)output_number, compact->compactionL0->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);*/
      Log(options_.info_log, "Generated tableL0 #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compactionL0->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFileL0(CompactionState* compact,
                                            Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      /*Log(options_.info_log, "%d Generated tableL0 #%llu@%d: %lld keys, %lld bytes",
          gettid(),(unsigned long long)output_number, compact->compactionL0->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);*/
      Log(options_.info_log, "Generated tableL0 #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compactionL0->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResultsL0(CompactionState* compact) {
  mutex_.AssertHeld();
  /*Log(options_.info_log, "%d,Compacted %d@%d + %d@%d files => %lld bytes",
      gettid(),1, compact->compactionL0->level(),
      compact->compactionL0->num_input_filesL1(), compact->compactionL0->level() + 1,
      static_cast<long long>(compact->total_bytes));*/
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compactionL0->num_input_filesL0(), compact->compactionL0->level(),
      compact->compactionL0->num_input_filesL1(), compact->compactionL0->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compactionL0->AddInputDeletions(compact->compactionL0->edit());
  const int level = compact->compactionL0->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compactionL0->edit()->AddFile(level + 1, out.number, out.file_size,
                                           out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compactionL0->edit(), &mutex_);
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}
void DBImpl::DoSubCompactionWorkL0(CompactionState* compact, int k, Iterator * input, std::vector<Status> & status){

  if(k==0) {
    input->SeekToFirst();
  } else {
    std::string start_key;
    AppendInternalKey(&start_key,
                      ParsedInternalKey(compact->compactionL0->memTable_->split_key_[k-1], kMaxSequenceNumber, kValueTypeForSeek));
    input->Seek(start_key);
  }
  std::string end_key;
  if(k < FIRST_L0_THREAD_NUMBER -1) {
    end_key = compact->compactionL0->memTable_->split_key_[k];
  }
  //Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    Slice key = input->key();
    if (compact->compactionL0->ShouldStopBeforeSub(key, k) &&
        compact->builders[k] != nullptr) {
      status[k] = FinishCompactionOutputFileL0(compact, input, k);
      if (!status[k].ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else if( k<FIRST_L0_THREAD_NUMBER -1 && internal_comparator_.user_comparator()->Compare(ikey.user_key, end_key)>=0){
      break;
    }else {
      if (key.data() >= page_base && key.data() <= page_end) {
        compact->pages_[k][(key.data() - page_base) / PAGE_SIZE]--;
      }
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compactionL0->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builders[k] == nullptr) {
        status[k] = OpenCompactionOutputFileSub(compact,k);
        if (!status[k].ok()) {
          break;
        }
      }
      if (compact->builders[k]->NumEntries() == 0) {
        compact->current_outputsub(k)->smallest.DecodeFrom(key);
      }
      compact->current_outputsub(k)->largest.DecodeFrom(key);
      compact->builders[k]->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builders[k]->FileSize() >=
          compact->compactionL0->MaxOutputFileSize()) {
        status[k] = FinishCompactionOutputFileL0(compact, input,k);
        if (!status[k].ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status[k].ok() && shutting_down_.load(std::memory_order_acquire)) {
    status[k] = Status::IOError("Deleting DB during compaction");
  }
  if (status[k].ok() && compact->builders[k] != nullptr) {
    status[k] = FinishCompactionOutputFileL0(compact, input,k);
  }
  if (status[k].ok()) {
    status[k] = input->status();
  }
  delete input;
  input = nullptr;
  //return status;

}
Status DBImpl::DoCompactionWorkL0(CompactionState* compact){
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  /*Log(options_.info_log, "%d,CompactingL0 %d@%d + %d@%d files",
      gettid(),compact->compactionL0->num_input_filesL0(), compact->compactionL0->level(),
      compact->compactionL0->num_input_filesL1(),
      compact->compactionL0->level() + 1);*/
  Log(options_.info_log, "CompactingL0 %d@%d + %d@%d files",
      compact->compactionL0->num_input_filesL0(), compact->compactionL0->level(),
      compact->compactionL0->num_input_filesL1(),
      compact->compactionL0->level() + 1);

  //assert(versions_->NumLevelFiles(compact->compactionL0->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }
  if(compact->compactionL0->memTable_->is_first_flush_) {
    std::vector<Iterator *>input(FIRST_L0_THREAD_NUMBER, nullptr);

    for(int i = 0; i < FIRST_L0_THREAD_NUMBER; i++) {
      input[i] = versions_->MakeInputIteratorL0(compact->compactionL0);
    }
    mutex_.Unlock();
    std::vector<Status>statuss(FIRST_L0_THREAD_NUMBER);
    Status status;
    std::vector<std::thread>threads;
    threads.reserve(FIRST_L0_THREAD_NUMBER - 1);
    for(int i = 1;i < FIRST_L0_THREAD_NUMBER; i++) {
      threads.emplace_back( [this, compact, i, input,&statuss]() {
        this->DoSubCompactionWorkL0(compact, i, input[i],statuss);  // 假设 input[i] 是需要传入的元素
      });
    }
    DoSubCompactionWorkL0(compact, 0, input[0],statuss);
    for(auto &thread:threads) {
      thread.join();
    }
    compact->merge();
    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros - imm_micros;
    //stats.bytes_read += compact->compactionL0->inputL0()->ApproximateMemoryUsage();
    for (int i = 0; i < compact->compactionL0->num_input_filesL1(); i++) {
      stats.bytes_read += compact->compactionL0->inputL1(i)->file_size;
    }

    for (size_t i = 0; i < compact->outputs.size(); i++) {
      stats.bytes_written += compact->outputs[i].file_size;
    }
    for(int i =0 ;i<FIRST_L0_THREAD_NUMBER;i++) {
      if(!statuss[i].ok()) {
        status= statuss[i];
        break;
      }
    }
    mutex_.Lock();
    //Log(options_.info_log, "install compaction lock");
    stats_[compact->compactionL0->level() + 1].Add(stats);

    if (status.ok()) {
      status = InstallCompactionResultsL0(compact);
    }
    if(status.ok()){
      compact->compactionL0->memTable_->SetPage(compact->page_);
      compact->compactionL0->memTable_->SetRole(MemTable::COMPACTTABLED);
      compaction_tables_.remove(compact->compactionL0->memTable_);
      compact->compactionL0->memTable_->Unref();
    }

    if (!status.ok()) {
      Log(options_.info_log,"L0->L1 ERROR");
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    /*Log(options_.info_log, "%d,compacted toL0: %s", gettid(),versions_->LevelSummary(&tmp));*/
    Log(options_.info_log, "compacted toL0: %s",versions_->LevelSummary(&tmp));
    do_first_compacting_ = false;
    return status;
  } else {
    Iterator* input = versions_->MakeInputIteratorL0(compact->compactionL0);

    // Release mutex while we're actually doing the compaction work
    mutex_.Unlock();

    input->SeekToFirst();
    Status status;
    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
      Slice key = input->key();
      if (key.data() >= page_base && key.data() <= page_end) {
        compact->page_[(key.data() - page_base) / PAGE_SIZE]--;
      }
      if (compact->compactionL0->ShouldStopBefore(key) &&
          compact->builder != nullptr) {
        status = FinishCompactionOutputFileL0(compact, input);
        if (!status.ok()) {
          break;
        }
      }

      // Handle key/value, add to state, etc.
      bool drop = false;
      if (!ParseInternalKey(key, &ikey)) {
        // Do not hide error keys
        current_user_key.clear();
        has_current_user_key = false;
        last_sequence_for_key = kMaxSequenceNumber;
      } else {
        if (!has_current_user_key ||
            user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
                0) {
          // First occurrence of this user key
          current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
          has_current_user_key = true;
          last_sequence_for_key = kMaxSequenceNumber;
        }

        if (last_sequence_for_key <= compact->smallest_snapshot) {
          // Hidden by an newer entry for same user key
          drop = true;  // (A)
        } else if (ikey.type == kTypeDeletion &&
                   ikey.sequence <= compact->smallest_snapshot &&
                   compact->compactionL0->IsBaseLevelForKey(ikey.user_key)) {
          // For this user key:
          // (1) there is no data in higher levels
          // (2) data in lower levels will have larger sequence numbers
          // (3) data in layers that are being compacted here and have
          //     smaller sequence numbers will be dropped in the next
          //     few iterations of this loop (by rule (A) above).
          // Therefore this deletion marker is obsolete and can be dropped.
          drop = true;
        }

        last_sequence_for_key = ikey.sequence;
      }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

      if (!drop) {
        // Open output file if necessary
        if (compact->builder == nullptr) {
          status = OpenCompactionOutputFile(compact);
          if (!status.ok()) {
            break;
          }
        }
        if (compact->builder->NumEntries() == 0) {
          compact->current_output()->smallest.DecodeFrom(key);
        }
        compact->current_output()->largest.DecodeFrom(key);
        compact->builder->Add(key, input->value());

        // Close output file if it is big enough
        if (compact->builder->FileSize() >=
            compact->compactionL0->MaxOutputFileSize()) {
          status = FinishCompactionOutputFileL0(compact, input);
          if (!status.ok()) {
            break;
          }
        }
      }

      input->Next();
    }


    if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
      status = Status::IOError("Deleting DB during compaction");
    }
    if (status.ok() && compact->builder != nullptr) {
      status = FinishCompactionOutputFileL0(compact, input);
    }
    if (status.ok()) {
      status = input->status();
    }
    delete input;
    input = nullptr;

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros - imm_micros;
    //stats.bytes_read += compact->compactionL0->inputL0()->ApproximateMemoryUsage();
    for (int i = 0; i < compact->compactionL0->num_input_filesL1(); i++) {
      stats.bytes_read += compact->compactionL0->inputL1(i)->file_size;
    }

    for (size_t i = 0; i < compact->outputs.size(); i++) {
      stats.bytes_written += compact->outputs[i].file_size;
    }

    mutex_.Lock();
    //Log(options_.info_log, "install compaction lock");
    stats_[compact->compactionL0->level() + 1].Add(stats);

    if (status.ok()) {
      status = InstallCompactionResultsL0(compact);
    }
    if(status.ok()){
      compact->compactionL0->memTable_->SetPage(compact->page_);
      compact->compactionL0->memTable_->SetRole(MemTable::COMPACTTABLED);
      compaction_tables_.remove(compact->compactionL0->memTable_);
      compact->compactionL0->memTable_->Unref();
    }

    if (!status.ok()) {
      Log(options_.info_log,"L0->L1 ERROR");
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    /*Log(options_.info_log, "%d,compacted toL0: %s", gettid(),versions_->LevelSummary(&tmp));*/
    Log(options_.info_log, "compacted toL0: %s",versions_->LevelSummary(&tmp));
    return status;



  }



}
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      //Log(options_.info_log, "flush memetable lock");
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  //Log(options_.info_log, "install compaction lock");
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

struct IterStateL0 {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  std::vector<MemTable *> table GUARDED_BY(mu);


  IterStateL0(port::Mutex* mutex, std::vector<MemTable*>&table, Version* version)
      : mu(mutex), version(version), table(std::move(table)) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

static void CleanupIteratorStateL0(void* arg1, void* arg2) {
  IterStateL0* state = reinterpret_cast<IterStateL0*>(arg1);
  state->mu->Lock();
  for(int i=0;i<state->table.size();i++){
    state->table[i]->Unref();
    if(state->table[i]->GetRole()== MemTable::BIGTABLE && state->table[i]->refs_ == 1) {
      state->table[i]->SetStatus(MemTable::WRITE);
      state->table[i]->db_->background_work_finished_signal_merge_.SignalAll();
    }


  }
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIteratorL0(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  //Log(options_.info_log, "new iter lock");
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  std::vector<MemTable*>table_list;
  //list.push_back(mem_->NewIterator());
  if(mem_) {
    list.push_back(mem_->NewIterator());
    table_list.push_back(mem_);
    mem_->Ref();
  }
  if(sort_table_) {
    list.push_back(sort_table_->NewIterator());
    table_list.push_back(sort_table_);
    sort_table_->Ref();
  }

  for(auto merge : merge_imm_) {
    list.push_back(merge->NewIterator());
    table_list.push_back(merge);
    merge->Ref();
  }
  if(big_table_) {
    list.push_back(big_table_->NewIterator());
    table_list.push_back(big_table_);
    big_table_->Ref();
    big_table_->SetStatus(MemTable::READ);
  }
  for(auto compaction : compaction_tables_) {
    list.push_back(compaction->NewIterator());
    table_list.push_back(compaction);
    compaction->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterStateL0* cleanup = new IterStateL0(&mutex_, table_list, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorStateL0, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  //Log(options_.info_log, "new iter lock");
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

bool DBImpl::Get(std::vector<MemTable*> &list, const LookupKey& key, std::string* value, Status* s){
  for(auto table:list){
    if(table->Get(key,value,s)){
      return true;
    }
  }
  return false;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }
  std::vector<MemTable*>list;
  if(mem_) {
    list.push_back(mem_);
    mem_->Ref();
  }
  if(sort_table_) {
    list.push_back(sort_table_);
    sort_table_->Ref();
  }
  for(auto merge:merge_imm_) {
    list.push_back(merge);
    merge->Ref();
  }
  if(big_table_) {
    list.push_back(big_table_);
    big_table_->Ref();
    big_table_->status_ = MemTable::READ;
  }
  for(auto compaction:compaction_tables_) {
    list.push_back(compaction);
    compaction->Ref();
  }
  Version* current = versions_->current();
  current->Ref();
  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if(!list.empty() && Get(list,lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
    //Log(options_.info_log, "get lock");
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }

  for(auto  table :list) {
    table->Unref();
  }
  if(big_table_ && big_table_->refs_ == 1) {
    big_table_->status_ = MemTable::WRITE;
    background_work_finished_signal_merge_.SignalAll();
  }
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIteratorL0(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}
  /*SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}*/

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  //Log(options_.info_log, "makeroomsuccess\n");
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      //Log(options_.info_log, "log write success: %d\n",status.ok());
      mutex_.Lock();
      //Log(options_.info_log, "log write success lock\n");
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
  //std::cout<<"1"<<std::endl;
  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && (merge_imm_.size() > 10|| nvmManager->get_free_page_number() <=100)) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      Log(options_.info_log, "merge long %d,or free page small %d...\n",merge_imm_.size(), nvmManager->get_free_page_number());
      env_->SleepForMicroseconds(500);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
      //Log(options_.info_log, "merge long %d,or free page small %d success lock...\n",merge_imm_.size(), nvmManager->get_free_page_number());
    } else
    if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (sort_table_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_imm_.Wait();
    } /*else if (PAGE_NUMBER - nvmManager->get_free_page_number() == 0) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_L0_.Wait();
    }*/ else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      delete log_;

      s = logfile_->Close();
      if (!s.ok()) {
        // We may have lost some data written to the previous log file.
        // Switch to the new log file anyway, but record as a background
        // error so we do not attempt any more writes.
        //
        // We could perhaps attempt to save the memtable corresponding
        // to log file and suppress the error if that works, but that
        // would add more complexity in a critical code path.
        RecordBackgroundError(s);
      }
      delete logfile_;

      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      sort_table_ = mem_;
      sort_table_->SetRole(MemTable::PEDDINGSORT);
      //has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_, this);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      background_work_finished_signal_sort_.SignalAll();
      /*MaybeScheduleCompactionL0();
      MaybeScheduleCompaction();*/
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  //Log(impl->options_.info_log, "open lock");
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_,impl);
      impl->mem_->Ref();
      impl->big_table_ = new MemTable(impl->internal_comparator_,impl,true, MemTable::BIGTABLE);
      impl->big_table_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
    impl->env_->StartThread([](void* arg) {
      static_cast<DBImpl*>(arg)->BackgroundTableCompaction();
    }, impl);
    impl->env_->StartThread([](void* arg) {
      static_cast<DBImpl*>(arg)->BackGroundTableSort();
    }, impl);
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
