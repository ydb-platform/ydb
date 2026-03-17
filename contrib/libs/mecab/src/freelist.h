//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_FREELIST_H
#define MECAB_FREELIST_H

#include <vector>
#include <algorithm>
#include "utils.h"
#include "common.h"

namespace MeCab {

template <class T> class FreeList {
 private:
  std::vector<T *> freeList;
  size_t           pi_;
  size_t           li_;
  size_t           size;

 public:
  void free() { li_ = pi_ = 0; }

  T* alloc() {
    if (pi_ == size) {
      li_++;
      pi_ = 0;
    }
    if (li_ == freeList.size()) freeList.push_back(new T[size]);
    return freeList[li_] + (pi_++);
  }

  explicit FreeList(size_t _size): pi_(0), li_(0), size(_size) {}

  virtual ~FreeList() {
    for (li_ = 0; li_ < freeList.size(); li_++)
      delete [] freeList[li_];
  }
};

template <class T> class ChunkFreeList {
 private:
  std::vector<std::pair<size_t, T *> > freelist_;
  size_t pi_;
  size_t li_;
  size_t default_size;

 public:
  void free() { li_ = pi_ = 0; }

  T* alloc(T *src) {
    T* n = alloc(1);
    *n = *src;
    return n;
  }

  T* alloc(size_t req = 1) {
    while (li_ < freelist_.size()) {
      if ((pi_ + req) < freelist_[li_].first) {
        T *r = freelist_[li_].second + pi_;
        pi_ += req;
        return r;
      }
      li_++;
      pi_ = 0;
    }
    size_t _size = std::max(req, default_size);
    freelist_.push_back(std::make_pair(_size, new T[_size]));
    li_ = freelist_.size() - 1;
    pi_ += req;
    return freelist_[li_].second;
  }

  explicit ChunkFreeList(size_t _size):
      pi_(0), li_(0), default_size(_size) {}

  virtual ~ChunkFreeList() {
    for (li_ = 0; li_ < freelist_.size(); li_++)
      delete [] freelist_[li_].second;
  }
};
}
#endif
