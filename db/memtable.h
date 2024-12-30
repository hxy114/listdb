// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include "db/dbformat.h"
#include "db/skiplist.h"
#include <string>

#include "leveldb/db.h"

#include "util/arena.h"
#include "util/nvm_arena.h"

#include "db_impl.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

class MemTable {
 public:
  enum Role {
    MEM,
    PEDDINGSORT,
    SORTING,
    SORTED,
    PEDDINGMERGE,
    MERGEING,
    MERGED,
    BIGTABLE,
    PEDDINGCOMPACT,
    COMPACTING,
    COMPACTTABLED
  };
  enum MyStatus {
    READ,
    WRITE
  };
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator& comparator, Role role = MEM);
  explicit MemTable(const InternalKeyComparator& comparator, DBImpl *db, bool big_table, Role role = MEM);
  explicit MemTable(const InternalKeyComparator& comparator, DBImpl *db, Role role = MEM);
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // Increase reference count.
  void Ref() {
    ++refs_;
    if(arena_ != nullptr) {
      arena_->Ref();
    }
    if(data_arena_ != nullptr) {
      data_arena_->Ref();
    }
    if(nvmArena_ != nullptr) {
      nvmArena_->Ref();
    }
  }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if(arena_ != nullptr && arena_->Unref()) {
      arena_= nullptr;
    }
    if(data_arena_ != nullptr && data_arena_->Unref()) {
      data_arena_ = nullptr;
    }
    if(nvmArena_ != nullptr && nvmArena_->Unref()) {
      nvmArena_ = nullptr;
    }
    if(role_>=SORTED && refs_ == 1 && data_arena_ != nullptr) {
      data_arena_->~Arena();
      data_arena_ = nullptr;
    }
    if (refs_ <= 0) {


      //assert(arena_== nullptr && data_arena_ == nullptr && nvmArena_ == nullptr && last_arena_ == nullptr);
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();
  bool is_empty();
  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool GetFirst(std::string *value);
  bool Get(const LookupKey& key, std::string* value, Status* s);
  void Sort();
  void ChangeArena();
  void Compaction(std::vector<MemTable*>&tables, DBImpl *db);
  MemTable * Split(Slice &start, Slice &end, bool is_start, bool &is_end);
 private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  friend class DBImpl;
  friend class VersionSet;

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef SkipList<const char*, KeyComparator> Table;
 public:
  MemTable(const InternalKeyComparator& comparator, Table* table, DBImpl *db, bool is_first_flush, Role role =MEM);
  Role GetRole();
  void SetRole(Role role);
  void SetStatus(MemTable::MyStatus status);
  void SetPage(std::map<size_t, int32_t> &page);
 public:

  ~MemTable();  // Private since only Unref() should be used to delete it

  KeyComparator comparator_;
  int refs_;
  Arena *arena_;
  Arena *data_arena_;
  NvmArena *nvmArena_;
  Table table_;
  Role role_;
  DBImpl *db_;
  std::map<size_t, int32_t> pages_;
  std::string start_key_;
  std::string end_key_;
  bool is_first_flush_;
  std::vector<std::string> split_key_;
  MyStatus status_;
  std::list<Arena*> last_arena_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
