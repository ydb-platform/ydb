// MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <cstdio>
#include <cstring>
#include "common.h"
#include "string_buffer.h"

#define DEFAULT_ALLOC_SIZE BUF_SIZE

namespace MeCab {

bool StringBuffer::reserve(size_t length) {
  if (!is_delete_) {
    error_ = (size_ + length >= alloc_size_);
    return (!error_);
  }

  if (size_ + length >= alloc_size_) {
    if (alloc_size_ == 0) {
      alloc_size_ = DEFAULT_ALLOC_SIZE;
      ptr_ = new char[alloc_size_];
    }
    size_t len = size_ + length;
    do {
      alloc_size_ *= 2;
    } while (len >= alloc_size_);
    char *new_ptr = new char[alloc_size_];
    std::memcpy(new_ptr, ptr_, size_);
    delete [] ptr_;
    ptr_ = new_ptr;
  }

  return true;
}

StringBuffer::~StringBuffer() {
  if (is_delete_) {
    delete [] ptr_;
    ptr_ = 0;
  }
}

StringBuffer& StringBuffer::write(char str) {
  if (reserve(1)) {
    ptr_[size_] = str;
    ++size_;
  }
  return *this;
}

StringBuffer& StringBuffer::write(const char* str) {
  return this->write(str, std::strlen(str));
}

StringBuffer& StringBuffer::write(const char* str, size_t length) {
  if (reserve(length)) {
    std::memcpy(ptr_ + size_ , str, length);
    size_ += length;
  }
  return *this;
}
}
