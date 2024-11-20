// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include "db/dbformat.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator, Role role)//最初始版本的memtable，应该废弃
    : comparator_(comparator), refs_(0), arena_(nullptr),data_arena_(nullptr), nvmArena_(nullptr),table_(comparator_, arena_) ,role_(role),db_(nullptr),is_first_flush_(false), status_(WRITE){

}
MemTable::MemTable(const InternalKeyComparator& comparator, DBImpl *db, Role role)//使用下面这个版本的memtable。
    : comparator_(comparator), refs_(0), arena_(new Arena), data_arena_(new Arena), nvmArena_(nullptr), table_(comparator_, arena_) ,role_(role),db_(db), status_(WRITE){
}
MemTable::MemTable(const InternalKeyComparator& comparator, DBImpl *db, bool big_table,Role role)//使用下面这个版本的memtable。
    : comparator_(comparator), refs_(0), arena_(new Arena), data_arena_(new Arena), nvmArena_(nullptr), table_(comparator_, arena_,true) ,role_(role),db_(db), status_(WRITE){
}
MemTable::MemTable(const InternalKeyComparator& comparator, Table* table, DBImpl* db, bool is_first_flush, Role role)//compaction的memtable
    : comparator_(comparator), refs_(0), arena_(nullptr), data_arena_(nullptr), nvmArena_(nullptr),table_(table), role_(role),db_(db), is_first_flush_(is_first_flush), status_(WRITE){
  arena_ = table_.arena_;
}

MemTable::~MemTable() {
  assert(refs_ == 0);
  if(role_ == COMPACTTABLED && MergeTable(pages_) > 0) {
    db_->background_work_finished_signal_sort_.SignalAll();
  }
}

size_t MemTable::ApproximateMemoryUsage() { return arena_->MemoryUsage() + data_arena_->MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice mykey() const  { return iter_.key(); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = data_arena_->Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf,encoded_len);
}

bool MemTable::GetFirst(std::string *value) {
  auto node = table_.head_->Next(0);

  if(node != nullptr) {
    const char* entry = node->key;
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    assert(key_length==24);
    value->assign(key_ptr, key_length- 8);
    return true;
  }
  return false;
}
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

void MemTable::Sort() {
  table_.Sort(nvmArena_, db_);
  auto page=nvmArena_->Merge();
  MergeTable(page);
  role_ = SORTED;
  if(refs_ == 1) {
    data_arena_->~Arena();
    data_arena_ = nullptr;
  }
}
void MemTable::ChangeArena() {
  last_arena_.push_back(arena_);
  arena_ = nullptr;
  if(last_arena_.size()>3) {
    last_arena_.front()->Unref();
    last_arena_.pop_front();
  }
  arena_ = new Arena;
  arena_->Ref();
  table_.arena_ = arena_;

}
void MemTable::Compaction(std::vector<MemTable*>&tables, DBImpl * db) {
  size_t num = tables.size();
  Iterator * list[num];
  for (size_t i = 0; i < num; i++) {
    list[i] = tables[i]->NewIterator();
  }
  Iterator* input = NewMergingIterator(&comparator_.comparator, list, num);

  input->SeekToFirst();
  //Table ::Node *pre[Table::kMaxHeight];
  int count =0;
  while (input->Valid()) {
    Slice key = input->mykey();
    Slice value = input->value();
    ParsedInternalKey result;
    //assert(ParseInternalKey(key,&result));
    //assert((long long int)(key.data()-(((key.data()-page_base)/PAGE_SIZE)*PAGE_SIZE +PAGE_HEAD_SIZE)) % 4123==0);
    table_.Insert(key.data(),key.size());//有问题
    //assert(key.size() == 24);
    input->Next();
    count ++;
    /*{
      auto iter = NewIterator();
      iter->SeekToFirst();
      auto keys = iter->key();
      assert(ParseInternalKey(keys,&result));
      assert(keys.size() == 24);
      delete iter;
    }*/
    if(count >100000) {
      db_->mutex_.Lock();
      //Log(db_->options_.info_log, "merge compaction  lock");
      while(db->HaveCompaction()) {
        bool result = db->GenerateCompaction();
        if(result) {
          ChangeArena();

        }
      }
      db_->mutex_.Unlock();
      count = 0;
    }
  }
  delete input;

  {
    auto iter =NewIterator();
    iter->SeekToFirst();
    int c=0;
    while(iter->Valid()) {
      c++;
      auto key = iter->key();
      ParsedInternalKey result;
      assert(ParseInternalKey(key,&result));
      assert(key.size() == 24);
      iter->Next();
    }
    assert(!iter->Valid());
    delete iter;
  }
}
MemTable * MemTable::Split(Slice &start, Slice &end,bool is_start, bool is_end) {
  Table  *new_table = nullptr;
  if(db_->is_first_flush_) {
    std::vector<void *>result;
    table_.FirstSplit(new_table,result);
    MemTable * memTable =new MemTable(comparator_.comparator,new_table,db_, true,PEDDINGCOMPACT);
    for(int i =0 ;i < result.size(); i++) {
      Slice key =GetLengthPrefixedSlice((char *)result[i]);
      memTable->split_key_.push_back(key.ToStringMy());
    }
    auto iter = memTable->NewIterator();
    iter->SeekToFirst();
    memTable->start_key_ = iter->key().ToStringMy();
    iter->SeekToLast();
    memTable->end_key_ = iter->key().ToStringMy();
    return memTable;
  } else {
    LookupKey s_k(start,0),e_k(end,0);
    table_.Split(s_k.memtable_key().data(), e_k.memtable_key().data(),new_table,is_start,is_end);
    MemTable * memTable =new MemTable(comparator_.comparator,new_table,db_, false,PEDDINGCOMPACT);
    auto iter = memTable->NewIterator();
    iter->SeekToFirst();
    memTable->start_key_ = iter->key().ToStringMy();
    iter->SeekToLast();
    memTable->end_key_ = iter->key().ToStringMy();
    return memTable;
  }


}

MemTable::Role MemTable::GetRole(){
  return role_;
}
void MemTable::SetStatus(MemTable::MyStatus status) {
  status_ = status;
}
void MemTable::SetRole(Role role){
  role_ =role;
}
void MemTable::SetPage(std::map<size_t, int32_t> &page) {
  pages_ = page;
}

}  // namespace leveldb
