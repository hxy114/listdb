//
// Created by hxy on 24-3-29.
//

#ifndef LEVELDB_PM_MODULE_H
#define LEVELDB_PM_MODULE_H
#include <cstdint>
#include <mutex>
#include <libpmem.h>
#include <map>
#include <vector>
#include "port/port.h"


namespace leveldb {
extern const size_t PM_SIZE;
extern const size_t PAGE_HEAD_SIZE;//pm log大小
extern const size_t PAGE_SIZE;
extern const size_t PERSIST_SIZE;//非强制刷写大小
extern const size_t PAGE_NUMBER;//TODO
extern const char * PM_FILE_NAME;
extern const uint32_t PAGE_MAGIC;
extern const uint32_t INVALID;

extern const bool IS_FLUSH;

extern const uint64_t L0_THREAD_NUMBER;
extern const uint64_t FIRST_L0_THREAD_NUMBER;

typedef  struct PageHead{//size=64B
  uint32_t magic_number;
  uint64_t kv_count;
  char padding[48];
}PageHead;

class NvmManager {
 public:
  NvmManager (bool is_recover_);
  ~NvmManager();
  PageHead * get_page();
  void free_page(PageHead * pm_log);
  //std::vector<std::pair<uint64_t ,PageHead *>>&& get_recover_page_nodes();
  char *get_base();
  size_t get_free_page_number();

  size_t pm_size_;//pm大小

  port::Mutex mutex_;
  char* base_;
  char *page_base_;


  std::vector<PageHead *>free_page_list_;

  //std::vector<std::pair<uint64_t ,PageHead *>>recover_page_list_;

};

void reset(PageHead *pm_log_head);

void set(PageHead *pm_log_head);

extern NvmManager *nvmManager;
extern std::map<size_t, int32_t> pageTable;
extern char *page_base;
extern char *page_end;
extern size_t MergeTable(const std::map<size_t,int32_t>&free_page);
}
#endif  // LEVELDB_PM_MODULE_H
