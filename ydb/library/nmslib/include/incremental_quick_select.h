/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef _INCREMENTAL_QUICK_SELECT_H_
#define _INCREMENTAL_QUICK_SELECT_H_

#include <vector>
#include <stack>
#include "global.h"

namespace similarity {

template <typename T>
class IncrementalQuickSelect {
 public:
  explicit IncrementalQuickSelect(std::vector<T>& x)
      : x_(x), n_(static_cast<int>(x.size())), idx_(0) {
    stk_.push(n_);
  }

  ~IncrementalQuickSelect() {}

  T GetNext() {
    CHECK(idx_ <= stk_.top());
    if (stk_.top() == idx_) {
      stk_.pop();
      return x_[idx_];
    } else {
      int pivot_idx = idx_;     // OR random[idx, stk.top() - 1]
      int pivot_idx_final = Partition(pivot_idx, idx_, stk_.top() - 1);
      stk_.push(pivot_idx_final);
      return GetNext();
    }
  }

  inline bool Next() {
    ++idx_;
    return idx_ < n_;
  }

 private:

  int Partition(int pivot_idx, int left, int right) {
    T pivot = x_[pivot_idx];
    for (;;) {
      while (x_[left] < pivot) ++left;
      while (x_[right] > pivot) --right;

      if (left >= right) {
        break;
      } else if (x_[left] == x_[right]) {
        ++left;
      } else if (left < right) {
        std::swap(x_[left], x_[right]);
      }
    }
    return right;
  }

  std::vector<T>& x_;
  const int n_;
  int idx_;
  std::stack<int> stk_;

  // disable copy and assign
  DISABLE_COPY_AND_ASSIGN(IncrementalQuickSelect);
};

}  // namespace similarity

#endif     //  _INCREMENTAL_QUICK_SELECT_H_

