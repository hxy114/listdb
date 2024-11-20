//
// Created by hxy on 24-3-29.
//
#include "util/nvm_arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

NvmArena::NvmArena(bool force,bool recover)
    :  current_page_head_(nullptr), force_(force),
      current_kv_alloc_ptr_(nullptr),alloc_bytes_remaining_(0),refs_(0){
      /*if(recover){
        memory_usage_=pm_log_start_->used_size;
        kv_alloc_ptr_=(char*)pm_log_start_+memory_usage_;
        last_persist_point_=kv_alloc_ptr_;
      }*/


}

NvmArena::~NvmArena() {
  //TODO maybe归还pmlog
}

char* NvmArena::Allocate(size_t bytes) {
      assert(bytes > 0);
      if (bytes <= alloc_bytes_remaining_) {
        char* result = current_kv_alloc_ptr_;
        current_kv_alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        ++current_page_head_->kv_count;
        return result;
      }
      if(current_page_head_ != nullptr) {
        pages[current_page_number_] = current_page_head_->kv_count;
        current_page_head_ = nullptr;
        alloc_bytes_remaining_ = 0;
      }
      if(AllocateNewPage()) {
        char* result = current_kv_alloc_ptr_;
        current_kv_alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        ++current_page_head_->kv_count;
        return result;
      }
      return nullptr;


  /*if(kv_alloc_ptr_-last_persist_point_>PERSIST_SIZE){
    Persist();
  }

  char *result=kv_alloc_ptr_;
  kv_alloc_ptr_+=bytes;
  memory_usage_+=bytes;
  return result;*/
}

char* NvmArena::AllocateAligned(size_t bytes) {
//  if(kv_alloc_ptr_-last_persist_point_>PERSIST_SIZE){
//    Persist();
//  }
//  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
//  static_assert((align & (align - 1)) == 0,
//                "Pointer size should be a power of 2");
//  size_t current_mod = reinterpret_cast<uintptr_t>(kv_alloc_ptr_) & (align - 1);
//  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
//  size_t needed = bytes + slop;
//  char* result;
//
//  result = kv_alloc_ptr_ + slop;
//  kv_alloc_ptr_ += needed;
//  memory_usage_+=needed;
//  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return nullptr;
}
/*void NvmArena::Persist(){
  if(force_||kv_alloc_ptr_-last_persist_point_>PERSIST_SIZE){
    PersistKV();
    PersistHead();
    last_persist_point_=kv_alloc_ptr_;
  }


}
void NvmArena::PersistKV(){
  pmem_persist(last_persist_point_,kv_alloc_ptr_-last_persist_point_);


}*/
std::map<size_t ,int32_t>& NvmArena::Merge() {
  if(current_page_head_) {
        pages[current_page_number_] = current_page_head_->kv_count;
  }
  return pages;
}
/*void NvmArena::PersistHead() {

  pmem_persist(&(page_start_->used_size),sizeof(page_start_->used_size));//持久化使用量
  pmem_persist(&(page_start_->kv_count),sizeof(page_start_->kv_count))

  //pmem_persist(head_start,PM_LOG_HEAD_SIZE);

}*/



bool NvmArena::AllocateNewPage() {
  PageHead* result = nvmManager->get_page();
  if (result) {
    result->magic_number = PAGE_MAGIC;
    result->kv_count = 0;
    current_page_number_ = ((char *)result - nvmManager->page_base_) / PAGE_SIZE;
    pages[current_page_number_] = 0;
    current_kv_alloc_ptr_ = (char *)result + PAGE_HEAD_SIZE;
    current_page_head_ = result;
    alloc_bytes_remaining_ = PAGE_SIZE - PAGE_HEAD_SIZE;

    return true;
  }
  return false;
}


}  // namespace leveldb