//
// Created by hxy on 24-3-29.
//

#ifndef LEVELDB_NVM_ARENA_H
#define LEVELDB_NVM_ARENA_H

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <libpmem.h>
#include "nvm_module.h"
namespace  leveldb{

class NvmArena {
 public:
  friend class DBImpl;
  NvmArena(bool force=false,bool recover=false);

  NvmArena(const NvmArena&) = delete;
  NvmArena& operator=(const NvmArena&) = delete;

  ~NvmArena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return pages.size()*PAGE_SIZE;
  }
  /*void Persist();
  void PersistKV();
  void PersistHead();*/

  std::map<size_t ,int32_t>& Merge();
  bool AllocateNewPage();
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  bool Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
      return true;
    }
    return false;
  }
 private:

  // Allocation state

  std::map<size_t, int32_t> pages;
  size_t current_page_number_;
  PageHead *current_page_head_;//start
  bool force_;//是否每次强制压缩
  //char *current_kv_start_;//kv开始存储的位置
  char *current_kv_alloc_ptr_;//当前分配的地址
  //char *last_persist_point_;//上一次持久化的地址
  size_t alloc_bytes_remaining_;
  int refs_;

  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?

};



}
#endif  // LEVELDB_NVM_ARENA_H
