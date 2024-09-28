/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Longda on 2021/4/13.
//
#include <errno.h>
#include <string.h>

#include "common/io/io.h"
#include "common/lang/mutex.h"
#include "common/lang/algorithm.h"
#include "common/log/log.h"
#include "common/math/crc.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/buffer/buffer_pool_log.h"
#include "storage/db/db.h"

using namespace common;

static const int MEM_POOL_ITEM_NUM = 20;

////////////////////////////////////////////////////////////////////////////////
// 输出文件头信息：总共多少页，其中已经占了多少页
string BPFileHeader::to_string() const
{
  stringstream ss;
  ss << "pageCount:" << page_count << ", allocatedCount:" << allocated_pages;
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////

BPFrameManager::BPFrameManager(const char *name) : allocator_(name) {}

RC BPFrameManager::init(int pool_num)
{
  // 初始化内存为 pool_num 个内存池，每个内存池默认 128 个帧
  int ret = allocator_.init(false, pool_num);
  if (ret == 0) {
    return RC::SUCCESS;
  }
  return RC::NOMEM;
}

RC BPFrameManager::cleanup()
{
  if (frames_.count() > 0) {
    return RC::INTERNAL;
  }

  frames_.destroy();
  return RC::SUCCESS;
}

// 释放帧，释放前写回磁盘，触发回调函数 purger
int BPFrameManager::purge_frames(int count, function<RC(Frame *frame)> purger)
{
  lock_guard<mutex> lock_guard(lock_);

  vector<Frame *> frames_can_purge;

  // 想要释放多少个帧（页面）
  if (count <= 0) {
    count = 1;
  }
  frames_can_purge.reserve(count);

  auto purge_finder = [&frames_can_purge, count](const FrameId &frame_id, Frame *const frame) {
    if (frame->can_purge()) {
      // 给当前页帧增加引用计数，防止重复删除
      frame->pin();
      frames_can_purge.push_back(frame);
      if (frames_can_purge.size() >= static_cast<size_t>(count)) {
        return false;  // 结束查找可释放帧
      }
    }
    return true;  // 继续查找可释放帧
  };

  // 它接受一个 lambda 函数作为参数，并反向遍历 frames_ 中的每个元素，对每个元素调用传入的 lambda 函数。
  // 方向：最近最少
  frames_.foreach_reverse(purge_finder);
  LOG_INFO("purge frames find %ld pages total", frames_can_purge.size());

  /// 当前还在frameManager的锁内，而 purger 是一个非常耗时的操作
  /// 他需要把脏页数据刷新到磁盘上去，所以这里会极大地降低并发度
  int freed_count = 0;
  for (Frame *frame : frames_can_purge) {
    RC rc = purger(frame);
    if (RC::SUCCESS == rc) {
      free_internal(frame->frame_id(), frame);
      freed_count++;
    } else {
      frame->unpin();
      LOG_WARN("failed to purge frame. frame_id=%s, rc=%s", 
               frame->frame_id().to_string().c_str(), strrc(rc));
    }
  }
  LOG_INFO("purge frame done. number=%d", freed_count);
  return freed_count;
}

Frame *BPFrameManager::get(int buffer_pool_id, PageNum page_num)
{
  // 哪一文件的哪一页 -> 帧
  FrameId frame_id(buffer_pool_id, page_num);

  lock_guard<mutex> lock_guard(lock_);    // 作用域结束自动释放
  return get_internal(frame_id);
}

Frame *BPFrameManager::get_internal(const FrameId &frame_id)
{
  Frame *frame = nullptr;
  (void)frames_.get(frame_id, frame);
  if (frame != nullptr) {
    // 增加引用数
    frame->pin();
  }
  return frame;
}

// 分配新的帧
Frame *BPFrameManager::alloc(int buffer_pool_id, PageNum page_num)
{
  // 创建帧（Id）
  FrameId frame_id(buffer_pool_id, page_num);

  lock_guard<mutex> lock_guard(lock_);

  Frame *frame = get_internal(frame_id);
  if (frame != nullptr) {
    // 已分配，无需再分配
    return frame;
  }

  // 执行分配，如果内存空间不足，可能需要extend()
  frame = allocator_.alloc();

  if (frame != nullptr) {
    // 
    ASSERT(frame->pin_count() == 0, "got an invalid frame that pin count is not 0. frame=%s", 
           frame->to_string().c_str());
    frame->set_buffer_pool_id(buffer_pool_id);
    frame->set_page_num(page_num);
    frame->pin();
    frames_.put(frame_id, frame);
  }
  return frame;
}

RC BPFrameManager::free(int buffer_pool_id, PageNum page_num, Frame *frame)
{
  FrameId frame_id(buffer_pool_id, page_num);

  lock_guard<mutex> lock_guard(lock_);
  return free_internal(frame_id, frame);
}

RC BPFrameManager::free_internal(const FrameId &frame_id, Frame *frame)
{
  Frame                *frame_source = nullptr;

  // 查找 frame_id 对应的 frame，查找结果放在 frame_source 中
  [[maybe_unused]] bool found        = frames_.get(frame_id, frame_source);

  // pin_count = 1表示初始值
  ASSERT(found && frame == frame_source && frame->pin_count() == 1,
      "failed to free frame. found=%d, frameId=%s, frame_source=%p, frame=%p, pinCount=%d, lbt=%s",
      found, frame_id.to_string().c_str(), frame_source, frame, frame->pin_count(), lbt());

  frame->set_page_num(-1);
  frame->unpin();
  frames_.remove(frame_id);
  allocator_.free(frame);
  return RC::SUCCESS;
}

// 列出所有指定文件的页面
list<Frame *> BPFrameManager::find_list(int buffer_pool_id)
{
  lock_guard<mutex> lock_guard(lock_);

  list<Frame *> frames;
  auto fetcher = [&frames, buffer_pool_id](const FrameId &frame_id, Frame *const frame) -> bool {
    
    if (buffer_pool_id == frame_id.buffer_pool_id()) {
      frame->pin();
      frames.push_back(frame);
    }
    return true;
  };
  frames_.foreach (fetcher);
  return frames;
}

////////////////////////////////////////////////////////////////////////////////

// 用于遍历BufferPool中的所有页面

BufferPoolIterator::BufferPoolIterator() {}
BufferPoolIterator::~BufferPoolIterator() {}
RC BufferPoolIterator::init(DiskBufferPool &bp, PageNum start_page /* = 0 */)
{
  bitmap_.init(bp.file_header_->bitmap, bp.file_header_->page_count);
  if (start_page <= 0) {
    current_page_num_ = -1;
  } else {
    current_page_num_ = start_page - 1;
  }
  return RC::SUCCESS;
}

bool BufferPoolIterator::has_next() { return bitmap_.next_setted_bit(current_page_num_ + 1) != -1; }

PageNum BufferPoolIterator::next()
{
  PageNum next_page = bitmap_.next_setted_bit(current_page_num_ + 1);
  if (next_page != -1) {
    current_page_num_ = next_page;
  }
  return next_page;
}

RC BufferPoolIterator::reset()
{
  current_page_num_ = 0;
  return RC::SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////
DiskBufferPool::DiskBufferPool(
    BufferPoolManager &bp_manager, BPFrameManager &frame_manager, DoubleWriteBuffer &dblwr_manager, LogHandler &log_handler)
    : bp_manager_(bp_manager), frame_manager_(frame_manager), dblwr_manager_(dblwr_manager), log_handler_(*this, log_handler)
{}

DiskBufferPool::~DiskBufferPool()
{
  close_file();
  LOG_INFO("disk buffer pool exit");
}

// 读取和分配 file_name 对应的文件头
// file_name 如 "miniob/db/sys/Account.data"，能够唯一定位一个文件
RC DiskBufferPool::open_file(const char *file_name)
{
  // 使用open函数以读写模式（O_RDWR）打开指定的文件（file_name）
  int fd = open(file_name, O_RDWR);

  if (fd < 0) {
    LOG_ERROR("Failed to open file %s, because %s.", file_name, strerror(errno));
    return RC::IOERR_ACCESS;
  }
  LOG_INFO("Successfully open buffer pool file %s.", file_name);

  file_name_ = file_name;
  file_desc_ = fd;

  Page header_page;
  
  // 从 file_desc_ 对应的文件中，读取指定长度(sizeof(header_page)) 到 header_page
  int ret = readn(file_desc_, &header_page, sizeof(header_page));
  if (ret != 0) {
    LOG_ERROR("Failed to read first page of %s, due to %s.", file_name, strerror(errno));
    close(fd);
    file_desc_ = -1;
    return RC::IOERR_READ;
  }

//  解析文件头 强制转换 文件头域内容
  BPFileHeader *tmp_file_header = reinterpret_cast<BPFileHeader *>(header_page.data);
  buffer_pool_id_ = tmp_file_header->buffer_pool_id;

// BP_HEADER_PAGE = 0，即文件头页号为0
// 为文件头分配帧到 hdr_frame_ 中
  RC rc = allocate_frame(BP_HEADER_PAGE, &hdr_frame_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to allocate frame for header. file name %s", file_name_.c_str());
    close(fd);
    file_desc_ = -1;
    return rc;
  }

  hdr_frame_->set_buffer_pool_id(id());

  // 刷新页面访问时间
  hdr_frame_->access();

  // 加载指定页面（file_desc_）到内存中（hdr_frame_）中
  if ((rc = load_page(BP_HEADER_PAGE, hdr_frame_)) != RC::SUCCESS) {
    LOG_ERROR("Failed to load first page of %s, due to %s.", file_name, strerror(errno));
    purge_frame(BP_HEADER_PAGE, hdr_frame_);
    close(fd);
    file_desc_ = -1;
    return rc;
  }

  file_header_ = (BPFileHeader *)hdr_frame_->data();

  LOG_INFO("Successfully open %s. file_desc=%d, hdr_frame=%p, file header=%s",
           file_name, file_desc_, hdr_frame_, file_header_->to_string().c_str());
  return RC::SUCCESS;
}

// 释放内存
RC DiskBufferPool::close_file()
{
  RC rc = RC::SUCCESS;

  // file_desc_ 对应当前bufferpool的文件
  if (file_desc_ < 0) {
    return rc;
  }

  hdr_frame_->unpin();

  // TODO: 理论上是在回放时回滚未提交事务，但目前没有undo log，因此不下刷数据page，只通过redo log回放
  rc = purge_all_pages();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to close %s, due to failed to purge pages. rc=%s", file_name_.c_str(), strrc(rc));
    return rc;
  }

  // 清空所有与指定buffer pool关联的页面
  rc = dblwr_manager_.clear_pages(this);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to clear pages in double write buffer. filename=%s, rc=%s", file_name_.c_str(), strrc(rc));
    return rc;
  }

  // 已经释放的页面
  disposed_pages_.clear();

  if (close(file_desc_) < 0) {
    LOG_ERROR("Failed to close fileId:%d, fileName:%s, error:%s", file_desc_, file_name_.c_str(), strerror(errno));
    return RC::IOERR_CLOSE;
  }
  LOG_INFO("Successfully close file %d:%s.", file_desc_, file_name_.c_str());
  file_desc_ = -1;

  bp_manager_.close_file(file_name_.c_str());
  return RC::SUCCESS;
}

RC DiskBufferPool:: remove_file_index(){
  // 调用之前删除数据文件时定义的删除方法
  bp_manager_.remove_file(file_name_.c_str());
  return RC::SUCCESS;
}

// 根据文件ID和页号 从磁盘 获取指定页面到缓冲区，返回页面句柄指针。
// 注意：申请bufferpool时，仅涉及文件头，不涉及具体页面
// 通过此处动态请求页面，请求所得在 *frame 中
RC DiskBufferPool::get_this_page(PageNum page_num, Frame **frame)
{
  RC rc  = RC::SUCCESS;
  *frame = nullptr;

  Frame *used_match_frame = frame_manager_.get(id(), page_num);
  if (used_match_frame != nullptr) {
    used_match_frame->access();
    *frame = used_match_frame;
    return RC::SUCCESS;
  }

  scoped_lock lock_guard(lock_);  // 直接加了一把大锁，其实可以根据访问的页面来细化提高并行度

  // Allocate one page and load the data into this page
  Frame *allocated_frame = nullptr;

  rc = allocate_frame(page_num, &allocated_frame);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to alloc frame %s:%d, due to failed to alloc page.", file_name_.c_str(), page_num);
    return rc;
  }

  allocated_frame->set_buffer_pool_id(id());
  // allocated_frame->pin(); // pined in manager::get
  allocated_frame->access();

  if ((rc = load_page(page_num, allocated_frame)) != RC::SUCCESS) {
    LOG_ERROR("Failed to load page %s:%d", file_name_.c_str(), page_num);
    purge_frame(page_num, allocated_frame);
    return rc;
  }

  *frame = allocated_frame;
  return RC::SUCCESS;
}

RC DiskBufferPool::allocate_page(Frame **frame)
{
  RC rc = RC::SUCCESS;

  lock_.lock();

  int byte = 0, bit = 0;

  // 还有未分配的页面
  if ((file_header_->allocated_pages) < (file_header_->page_count)) {
    for (int i = 0; i < file_header_->page_count; i++) {
      byte = i / 8;
      bit  = i % 8;
      // 未分配
      if (((file_header_->bitmap[byte]) & (1 << bit)) == 0) {
        (file_header_->allocated_pages)++;
        file_header_->bitmap[byte] |= (1 << bit);
        // TODO,  do we need clean the loaded page's data?
        // 在bufferpool中分配页面，相当于在磁盘中分配空闲页面
        // 因此导致状态不一致，标记脏
        hdr_frame_->mark_dirty();   
        LSN lsn = 0;
        rc = log_handler_.allocate_page(i, lsn);
        if (OB_FAIL(rc)) {
          LOG_ERROR("Failed to log allocate page %d, rc=%s", i, strrc(rc));
          // 忽略了错误
        }

        hdr_frame_->set_lsn(lsn);

        lock_.unlock();
        return get_this_page(i, frame);  // 返回新分配的页面给调用者
      }
    }
  }

  //当前文件容量不足，直接放回
  if (file_header_->page_count >= BPFileHeader::MAX_PAGE_NUM) {
    LOG_WARN("file buffer pool is full. page count %d, max page count %d",
        file_header_->page_count, BPFileHeader::MAX_PAGE_NUM);
    lock_.unlock();
    return RC::BUFFERPOOL_NOBUF;
  }

  // 在容量充足下，申请新页面，由于之前已申请的页面都被占据，因此新页面下标为 file_header_->page_count
  LSN lsn = 0;
  rc = log_handler_.allocate_page(file_header_->page_count, lsn);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to log allocate page %d, rc=%s", file_header_->page_count, strrc(rc));
    // 忽略了错误
  }
  hdr_frame_->set_lsn(lsn);

  PageNum page_num        = file_header_->page_count;
  Frame  *allocated_frame = nullptr;
  // 缓存帧
  if ((rc = allocate_frame(page_num, &allocated_frame)) != RC::SUCCESS) {
    LOG_ERROR("Failed to allocate frame %s, due to no free page.", file_name_.c_str());
    lock_.unlock();
    return rc;
  }

  LOG_INFO("allocate new page. file=%s, pageNum=%d, pin=%d",
           file_name_.c_str(), page_num, allocated_frame->pin_count());

  file_header_->allocated_pages++;
  file_header_->page_count++;

  byte = page_num / 8;
  bit  = page_num % 8;
  file_header_->bitmap[byte] |= (1 << bit);
  hdr_frame_->mark_dirty();

  allocated_frame->set_buffer_pool_id(id());
  allocated_frame->access();
  allocated_frame->clear_page();
  allocated_frame->set_page_num(file_header_->page_count - 1);

  // Use flush operation to extension file
  if ((rc = flush_page_internal(*allocated_frame)) != RC::SUCCESS) {
    LOG_WARN("Failed to alloc page %s , due to failed to extend one page.", file_name_.c_str());
    // skip return false, delay flush the extended page
    // return tmp;
  }

  lock_.unlock();

  *frame = allocated_frame;
  return RC::SUCCESS;
}

// 释放页面（释放内存的帧 + 释放磁盘文件的页面）
RC DiskBufferPool::dispose_page(PageNum page_num)
{
  if (page_num == 0) {
    LOG_ERROR("Failed to dispose page %d, because it is the first page. filename=%s", page_num, file_name_.c_str());
    return RC::INTERNAL;
  }
  
  scoped_lock lock_guard(lock_);
  Frame           *used_frame = frame_manager_.get(id(), page_num);
  if (used_frame != nullptr) {
    ASSERT("the page try to dispose is in use. frame:%s", used_frame->to_string().c_str());
    frame_manager_.free(id(), page_num, used_frame);
  } else {
    LOG_DEBUG("page not found in memory while disposing it. pageNum=%d", page_num);
  }

  LSN lsn = 0;
  RC rc = log_handler_.deallocate_page(page_num, lsn);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to log deallocate page %d, rc=%s", page_num, strrc(rc));
    // ignore error handle
  }

  hdr_frame_->set_lsn(lsn);
  hdr_frame_->mark_dirty();
  file_header_->allocated_pages--;
  char tmp = 1 << (page_num % 8);
  file_header_->bitmap[page_num / 8] &= ~tmp;
  return RC::SUCCESS;
}

RC DiskBufferPool::unpin_page(Frame *frame)
{
  frame->unpin();
  return RC::SUCCESS;
}

RC DiskBufferPool::purge_frame(PageNum page_num, Frame *buf)
{
  if (buf->pin_count() != 1) {
    LOG_INFO("Begin to free page %d frame_id=%s, but it's pin count > 1:%d.",
        buf->page_num(), buf->frame_id().to_string().c_str(), buf->pin_count());
    return RC::LOCKED_UNLOCK;
  }

  if (buf->dirty()) {
    RC rc = flush_page_internal(*buf);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to flush page %d frame_id=%s during purge page.", buf->page_num(), buf->frame_id().to_string().c_str());
      return rc;
    }
  }

  LOG_DEBUG("Successfully purge frame =%p, page %d frame_id=%s", buf, buf->page_num(), buf->frame_id().to_string().c_str());
  frame_manager_.free(id(), page_num, buf);
  return RC::SUCCESS;
}

RC DiskBufferPool::purge_page(PageNum page_num)
{
  scoped_lock lock_guard(lock_);

  Frame           *used_frame = frame_manager_.get(id(), page_num);
  if (used_frame != nullptr) {
    return purge_frame(page_num, used_frame);
  }

  return RC::SUCCESS;
}

RC DiskBufferPool::purge_all_pages()
{
  list<Frame *> used = frame_manager_.find_list(id());

  scoped_lock lock_guard(lock_);
  for (list<Frame *>::iterator it = used.begin(); it != used.end(); ++it) {
    Frame *frame = *it;

    purge_frame(frame->page_num(), frame);
  }
  return RC::SUCCESS;
}

RC DiskBufferPool::check_all_pages_unpinned()
{
  list<Frame *> frames = frame_manager_.find_list(id());

  scoped_lock lock_guard(lock_);
  for (Frame *frame : frames) {
    frame->unpin();
    if (frame->page_num() == BP_HEADER_PAGE && frame->pin_count() > 1) {
      LOG_WARN("This page has been pinned. id=%d, pageNum:%d, pin count=%d",
          id(), frame->page_num(), frame->pin_count());
    } else if (frame->page_num() != BP_HEADER_PAGE && frame->pin_count() > 0) {
      LOG_WARN("This page has been pinned. id=%d, pageNum:%d, pin count=%d",
          id(), frame->page_num(), frame->pin_count());
    }
  }
  LOG_INFO("all pages have been checked of id %d", id());
  return RC::SUCCESS;
}

RC DiskBufferPool::flush_page(Frame &frame)
{
  scoped_lock lock_guard(lock_);
  return flush_page_internal(frame);
}

// 如果页面是脏的，就将数据（从内存的帧 frame）刷新到磁盘
RC DiskBufferPool::flush_page_internal(Frame &frame)
{
  // The better way is use mmap the block into memory,
  // so it is easier to flush data to file.

  RC rc = log_handler_.flush_page(frame.page());
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to log flush frame= %s, rc=%s", frame.to_string().c_str(), strrc(rc));
    // ignore error handle
  }

  // 校验
  frame.set_check_sum(crc32(frame.page().data, BP_PAGE_DATA_SIZE));

  // this对应某个文件
  // frame.page_num()对应文件的页面
  // frame.page() 为待写回的文件内容
  // 先放到 double_write_buffer
  rc = dblwr_manager_.add_page(this, frame.page_num(), frame.page());
  if (OB_FAIL(rc)) {
    return rc;
  }

  frame.clear_dirty();
  LOG_DEBUG("Flush block. file desc=%d, frame=%s", file_desc_, frame.to_string().c_str());

  return RC::SUCCESS;
}

RC DiskBufferPool::flush_all_pages()
{
  list<Frame *> used = frame_manager_.find_list(id());
  for (Frame *frame : used) {
    RC rc = flush_page(*frame);
    frame->unpin();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to flush all pages");
      return rc;
    }
  }
  return RC::SUCCESS;
}

// 将页面标记为已分配
RC DiskBufferPool::recover_page(PageNum page_num)
{
  int byte = 0, bit = 0;
  byte = page_num / 8;
  bit  = page_num % 8;

  scoped_lock lock_guard(lock_);
  if (!(file_header_->bitmap[byte] & (1 << bit))) {
    file_header_->bitmap[byte] |= (1 << bit);
    file_header_->allocated_pages++;
    file_header_->page_count++;
    hdr_frame_->mark_dirty();
  }
  return RC::SUCCESS;
}

RC DiskBufferPool::write_page(PageNum page_num, Page &page)
{
  scoped_lock lock_guard(wr_lock_);
  int64_t     offset = ((int64_t)page_num) * sizeof(Page);
  if (lseek(file_desc_, offset, SEEK_SET) == -1) {
    LOG_ERROR("Failed to write page %lld of %d due to failed to seek %s.", offset, file_desc_, strerror(errno));
    return RC::IOERR_SEEK;
  }

  // 一次性写入数据 
  // 哪个文件 写入的数据页 大小
  if (writen(file_desc_, &page, sizeof(Page)) != 0) {
    LOG_ERROR("Failed to write page %lld of %d due to %s.", offset, file_desc_, strerror(errno));
    return RC::IOERR_WRITE;
  }

  LOG_TRACE("write_page: buffer_pool_id:%d, page_num:%d, lsn=%d, check_sum=%d", id(), page_num, page.lsn, page.check_sum);
  return RC::SUCCESS;
}

RC DiskBufferPool::redo_allocate_page(LSN lsn, PageNum page_num)
{
  // 最近已修改的大于等于指定的点，说明无需重做
  if (hdr_frame_->lsn() >= lsn) {
    return RC::SUCCESS;
  }

  // scoped_lock lock_guard(lock_); // redo 过程中可以不加锁
  if (page_num < file_header_->page_count) {
    Bitmap bitmap(file_header_->bitmap, file_header_->page_count);
    if (bitmap.get_bit(page_num)) {
      LOG_WARN("page %d has been allocated. file=%s", page_num, file_name_.c_str());
      return RC::SUCCESS;
    }

    bitmap.set_bit(page_num);
    file_header_->allocated_pages++;
    hdr_frame_->mark_dirty();
    return RC::SUCCESS;
  }

  if (page_num > file_header_->page_count) {
    LOG_WARN("page %d is not continuous. file=%s, page_count=%d",
             page_num, file_name_.c_str(), file_header_->page_count);
    return RC::INTERNAL;
  }

  // page_num == file_header_->page_count
  if (file_header_->page_count >= BPFileHeader::MAX_PAGE_NUM) {
    LOG_WARN("file buffer pool is full. page count %d, max page count %d",
        file_header_->page_count, BPFileHeader::MAX_PAGE_NUM);
    return RC::INTERNAL;
  }

  file_header_->allocated_pages++;
  file_header_->page_count++;
  hdr_frame_->set_lsn(lsn);
  hdr_frame_->mark_dirty();
  
  // TODO 应该检查文件是否足够大，包含了当前新分配的页面

  Bitmap bitmap(file_header_->bitmap, file_header_->page_count);
  bitmap.set_bit(page_num);
  LOG_TRACE("[redo] allocate new page. file=%s, pageNum=%d", file_name_.c_str(), page_num);
  return RC::SUCCESS;
}

RC DiskBufferPool::redo_deallocate_page(LSN lsn, PageNum page_num)
{
  if (hdr_frame_->lsn() >= lsn) {
    return RC::SUCCESS;
  }

  if (page_num >= file_header_->page_count) {
    LOG_WARN("page %d is not exist. file=%s", page_num, file_name_.c_str());
    return RC::INTERNAL;
  }

  Bitmap bitmap(file_header_->bitmap, file_header_->page_count);
  if (!bitmap.get_bit(page_num)) {
    LOG_WARN("page %d has been deallocated. file=%s", page_num, file_name_.c_str());
    return RC::INTERNAL;
  }

  bitmap.clear_bit(page_num);
  file_header_->allocated_pages--;
  hdr_frame_->set_lsn(lsn);
  hdr_frame_->mark_dirty();
  LOG_TRACE("[redo] deallocate page. file=%s, pageNum=%d", file_name_.c_str(), page_num);
  return RC::SUCCESS;
}

RC DiskBufferPool::allocate_frame(PageNum page_num, Frame **buffer)
{
  // this -> 当前文件
  // 遍历内存的每一帧
  auto purger = [this](Frame *frame) {
    if (!frame->dirty()) {
      return RC::SUCCESS;
    }

    // 页面是脏的
    RC rc = RC::SUCCESS;
    if (frame->buffer_pool_id() == id()) {
      rc = this->flush_page_internal(*frame);
    } else {
      // 如果不属于本缓冲池，则只能调用统一的 bp_manager_
      rc = bp_manager_.flush_page(*frame);
    }

    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to aclloc block due to failed to flush old block. rc=%s", strrc(rc));
    }
    return rc;
  };

  while (true) {
    Frame *frame = frame_manager_.alloc(id(), page_num);
    if (frame != nullptr) {
      *buffer = frame;
      LOG_DEBUG("allocate frame %p, page num %d", frame, page_num);
      return RC::SUCCESS;
    }

    //内存空闲空间不足
    LOG_TRACE("frames are all allocated, so we should purge some frames to get one free frame");
    (void)frame_manager_.purge_frames(1 /*count*/, purger);
  }

  return RC::BUFFERPOOL_NOBUF;
}

RC DiskBufferPool::check_page_num(PageNum page_num)
{
  if (page_num >= file_header_->page_count) {
    LOG_ERROR("Invalid pageNum:%d, file's name:%s", page_num, file_name_.c_str());
    return RC::BUFFERPOOL_INVALID_PAGE_NUM;
  }
  if ((file_header_->bitmap[page_num / 8] & (1 << (page_num % 8))) == 0) {
    LOG_ERROR("Invalid pageNum:%d, file's name:%s", page_num, file_name_.c_str());
    return RC::BUFFERPOOL_INVALID_PAGE_NUM;
  }
  return RC::SUCCESS;
}

// 加载指定页面的数据到内存中
RC DiskBufferPool::load_page(PageNum page_num, Frame *frame)
{
  Page &page = frame->page();
  // 如果从双重写入缓冲区成功读取到页面（OB_SUCC(rc)为真），则直接返回成功
  RC rc = dblwr_manager_.read_page(this, page_num, page);
  if (OB_SUCC(rc)) {
    return rc;
  }

  scoped_lock lock_guard(wr_lock_);
  int64_t          offset = ((int64_t)page_num) * BP_PAGE_SIZE;

  // 将file_desc_的文件位置从文件的开头，当前位置
  if (lseek(file_desc_, offset, SEEK_SET) == -1) {
    LOG_ERROR("Failed to load page %s:%d, due to failed to lseek:%s.", file_name_.c_str(), page_num, strerror(errno));

    return RC::IOERR_SEEK;
  }

  // 一次性读取指定长度的数据
  int ret = readn(file_desc_, &page, BP_PAGE_SIZE);
  if (ret != 0) {
    LOG_ERROR("Failed to load page %s, file_desc:%d, page num:%d, due to failed to read data:%s, ret=%d, page count=%d",
              file_name_.c_str(), file_desc_, page_num, strerror(errno), ret, file_header_->allocated_pages);
    return RC::IOERR_READ;
  }

  frame->set_page_num(page_num);

  LOG_DEBUG("Load page %s:%d, file_desc:%d, frame=%s",
            file_name_.c_str(), page_num, file_desc_, frame->to_string().c_str());
  return RC::SUCCESS;
}

int DiskBufferPool::file_desc() const { return file_desc_; }

////////////////////////////////////////////////////////////////////////////////
BufferPoolManager::BufferPoolManager(int memory_size /* = 0 */)
{
  if (memory_size <= 0) {
    memory_size = MEM_POOL_ITEM_NUM * DEFAULT_ITEM_NUM_PER_POOL * BP_PAGE_SIZE;
  }
  const int pool_num = max(memory_size / BP_PAGE_SIZE / DEFAULT_ITEM_NUM_PER_POOL, 1);
  // 为内存分配 pool_num 个内存池大小空间
  frame_manager_.init(pool_num);

  LOG_INFO("buffer pool manager init with memory size %d, page num: %d, pool num: %d",
           memory_size, pool_num * DEFAULT_ITEM_NUM_PER_POOL, pool_num);
}

BufferPoolManager::~BufferPoolManager()
{
  unordered_map<string, DiskBufferPool *> tmp_bps;
  tmp_bps.swap(buffer_pools_);

  for (auto &iter : tmp_bps) {
    delete iter.second;
  }
}

RC BufferPoolManager::init(unique_ptr<DoubleWriteBuffer> dblwr_buffer)
{
  dblwr_buffer_ = std::move(dblwr_buffer);
  return RC::SUCCESS;
}

// 新建文件写回磁盘
RC BufferPoolManager::create_file(const char *file_name)
{
  int fd = open(file_name, O_RDWR | O_CREAT | O_EXCL, S_IREAD | S_IWRITE);
  if (fd < 0) {
    LOG_ERROR("Failed to create %s, due to %s.", file_name, strerror(errno));
    return RC::SCHEMA_DB_EXIST;
  }

  close(fd);

  /**
   * Here don't care about the failure
   */
  fd = open(file_name, O_RDWR);
  if (fd < 0) {
    LOG_ERROR("Failed to open for readwrite %s, due to %s.", file_name, strerror(errno));
    return RC::IOERR_ACCESS;
  }

  // 创建文件头页面
  Page page;
  memset(&page, 0, BP_PAGE_SIZE);

  BPFileHeader *file_header    = (BPFileHeader *)page.data;
  file_header->allocated_pages = 1;
  file_header->page_count      = 1;
  file_header->buffer_pool_id  = next_buffer_pool_id_.fetch_add(1);

  char *bitmap = file_header->bitmap;
  bitmap[0] |= 0x01;  // 仅文件头有效，其余页面无效
  if (lseek(fd, 0, SEEK_SET) == -1) {
    LOG_ERROR("Failed to seek file %s to position 0, due to %s .", file_name, strerror(errno));
    close(fd);
    return RC::IOERR_SEEK;
  }
  
  // 文件头写回磁盘
  if (writen(fd, (char *)&page, BP_PAGE_SIZE) != 0) {
    LOG_ERROR("Failed to write header to file %s, due to %s.", file_name, strerror(errno));
    close(fd);
    return RC::IOERR_WRITE;
  }

  close(fd);
  LOG_INFO("Successfully create %s.", file_name);
  return RC::SUCCESS;
}

RC BufferPoolManager::open_file(LogHandler &log_handler, const char *_file_name, DiskBufferPool *&_bp)
{
  string file_name(_file_name);

  scoped_lock lock_guard(lock_);
  if (buffer_pools_.find(file_name) != buffer_pools_.end()) {
    LOG_WARN("file already opened. file name=%s", _file_name);
    return RC::BUFFERPOOL_OPEN;
  }

  // 一个文件代表一个 DiskBufferPool
  DiskBufferPool *bp = new DiskBufferPool(*this, frame_manager_, *dblwr_buffer_, log_handler);
  RC              rc = bp->open_file(_file_name);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open file name");
    delete bp;
    return rc;
  }

  // 系统启动时，会打开所有的表，这样就可以知道当前系统最大的ID（文件数）是多少了
  if (bp->id() >= next_buffer_pool_id_.load()) {
    next_buffer_pool_id_.store(bp->id() + 1);
  }

  // 记录所有的文件（buffetpool）
  buffer_pools_.insert(pair<string, DiskBufferPool *>(file_name, bp));
  // 记录所有的文件号
  id_to_buffer_pools_.insert(pair<int32_t, DiskBufferPool *>(bp->id(), bp));
  LOG_DEBUG("insert buffer pool into fd buffer pools. fd=%d, bp=%p, lbt=%s", bp->file_desc(), bp, lbt());
  _bp = bp;
  return RC::SUCCESS;
}

// 释放bufferpool
// 将此文件从bufferpool抹去（已确保文件不脏）
// 注意，与removefile不同，此处仅抹去内存，不抹去磁盘
RC BufferPoolManager::close_file(const char *_file_name)
{
  string file_name(_file_name);

  lock_.lock();

  auto iter = buffer_pools_.find(file_name);
  // 找不到该文件对应的bufferpool
  if (iter == buffer_pools_.end()) {
    LOG_TRACE("file has not opened: %s", _file_name);
    lock_.unlock();
    return RC::INTERNAL;
  }

  id_to_buffer_pools_.erase(iter->second->id());

  DiskBufferPool *bp = iter->second;
  buffer_pools_.erase(iter);
  lock_.unlock();

  delete bp;
  return RC::SUCCESS;
}

RC BufferPoolManager::remove_file(const char *file_name){
  //若文件在内存中，先从内存抹去
  close_file(file_name);
  if (::remove(file_name) != 0) { // 尝试删除文件
    LOG_ERROR("Fail to delete file, filename = %s, errmsg = %s",file_name,strerror(errno));
    return RC::INTERNAL;
  }
  return RC::SUCCESS;
}

RC BufferPoolManager::flush_page(Frame &frame)
{
  int buffer_pool_id = frame.buffer_pool_id();

  scoped_lock lock_guard(lock_);
  auto             iter = id_to_buffer_pools_.find(buffer_pool_id);
  if (iter == id_to_buffer_pools_.end()) {
    LOG_WARN("unknown buffer pool of id %d", buffer_pool_id);
    return RC::INTERNAL;
  }

  // 查询此帧对应的bufferpool
  DiskBufferPool *bp = iter->second;
  return bp->flush_page(frame);
}

RC BufferPoolManager::get_buffer_pool(int32_t id, DiskBufferPool *&bp)
{
  bp = nullptr;

  scoped_lock lock_guard(lock_);

  auto iter = id_to_buffer_pools_.find(id);
  if (iter == id_to_buffer_pools_.end()) {
    LOG_WARN("unknown buffer pool of id %d", id);
    return RC::INTERNAL;
  }
  
  bp = iter->second;
  return RC::SUCCESS;
}

