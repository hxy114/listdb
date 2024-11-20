//
// Created by hxy on 24-3-29.
//
#include "nvm_module.h"
#include <iostream>
#include <map>
namespace leveldb {
NvmManager *nvmManager= nullptr;
char *page_base = nullptr;
char *page_end = nullptr;
std::map<size_t, int32_t> pageTable;
port::Mutex pageTableMutex;
const size_t PM_SIZE=8*1024*1024*1024UL;
const size_t PAGE_HEAD_SIZE=64;//pm log大小
const size_t PAGE_SIZE=1*1024*1024UL;
const size_t PERSIST_SIZE=4*1024*1024;//非强制刷写大小
const size_t PAGE_NUMBER=8192;//TODO
const char * PM_FILE_NAME="/mnt/pmemdir/pm_log";
const uint32_t PAGE_MAGIC=0x0101;
const uint32_t INVALID=0x00;







const bool IS_FLUSH=true;

const uint64_t L0_THREAD_NUMBER=1;
const uint64_t FIRST_L0_THREAD_NUMBER = 3;


PageHead * NvmManager::get_page() {
  mutex_.Lock();
  PageHead *page_head= nullptr;
  if(!free_page_list_.empty()){
    page_head=free_page_list_.back();
    free_page_list_.pop_back();
  }
  mutex_.Unlock();
  return page_head;

}

void NvmManager::free_page(PageHead* pm_log) {
  mutex_.Lock();
  free_page_list_.emplace_back(pm_log);
  mutex_.Unlock();
}


NvmManager::NvmManager (bool is_recover_){
  size_t map_len;
  int is_pmem;
  if((base_= (char *)pmem_map_file(PM_FILE_NAME,PM_SIZE,PMEM_FILE_CREATE,0666,&map_len,&is_pmem))==NULL){
    perror("pmem_map_file");
    exit(1);
  }
  if(map_len!=PM_SIZE){
    perror("memory size not enough");
    pmem_unmap(base_,pm_size_);
    exit(1);
  }
  page_base_=base_;
  page_base = base_;
  page_end = base_+map_len;
  /*if(is_recover_){
    //TODO recover
    for(int i=0;i<PM_META_NODE_NUMBER;i++){
       auto *meta_node=(MetaNode*)(base_+i*sizeof(MetaNode));
      if(meta_node->magic_number==META_NODE_MAGIC){
        recover_meta_nodes_.emplace_back(i*sizeof(MetaNode),meta_node);

      }else{
        reset(meta_node);
        free_meta_node_list_.emplace_back(meta_node);
      }

    }
    for(int i=0;i<PM_LOG_NUMBER;i++){
      PmLogHead *pm_log_head=(PmLogHead*)(pm_log_base_+i*PM_LOG_SIZE);
      if(pm_log_head->magic_number==PM_LOG_MAGIC){
        recover_pm_log_list_.emplace_back(PM_META_NODE_NUMBER*sizeof(MetaNode)+i*PM_LOG_SIZE,pm_log_head);
      }else{
        reset(pm_log_head);
        free_pm_log_list_.emplace_back(pm_log_head);
      }

    }
    L0_wait_=(free_pm_log_list_.size()-MAX_PARTITION)*0.3;
    L0_stop_=0;



  }else{*/

    for(int i=0;i<PAGE_NUMBER;i++){
      PageHead *page_head= (PageHead*)(page_base_+i*PAGE_SIZE);
      reset(page_head);
      free_page_list_.emplace_back(page_head);
    }




  //}
}
NvmManager::~NvmManager(){
  pmem_drain();
  pmem_unmap(base_,PM_SIZE);
}

/*std::vector<std::pair<uint64_t ,PmLogHead *>>&& NvmManager::get_recover_pm_log_nodes_(){
  return std::move(recover_pm_log_list_);
}*/
char * NvmManager::get_base() {
  return base_;
}
size_t NvmManager::get_free_page_number(){
  return free_page_list_.size();
}


void reset(PageHead *pm_log_head){
  pmem_memset_persist(pm_log_head,0,sizeof(PageHead));
}

size_t MergeTable(const std::map<size_t,int32_t>&free_page) {
  size_t ret = 0;
  pageTableMutex.Lock();
  for(auto pair:free_page){
      pageTable[pair.first] +=pair.second;
      assert(pageTable[pair.first]>=0);
      if(pageTable[pair.first]==0){
        PageHead *head = (PageHead*) (nvmManager->page_base_ + PAGE_SIZE * pair.first);
        reset(head);
        nvmManager->free_page(head);
        ret++;
      }
  }
  pageTableMutex.Unlock();
  //std::cout<<"freepage:"<<free_page.size()<<" "<<ret<<std::endl;
  return ret;

}

}