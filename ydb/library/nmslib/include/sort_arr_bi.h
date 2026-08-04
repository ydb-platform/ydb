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
#ifndef  SORT_ARR_BI_H
#define  SORT_ARR_BI_H

#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <vector>
#include <algorithm>

#include "portable_simd.h"
#include "portable_prefetch.h"

/*
 * This is not a fully functional heap and this is done on purpose.
 */
template <typename KeyType, typename DataType>
class SortArrBI {
 public:
  class Item {
   public:
    KeyType   key;
    bool      used = false;
    DataType  data;

    Item() {}
    Item(const KeyType& k) : key(k) {}
    Item(const KeyType& k, const DataType& d) : key(k), data(d) {}

    bool operator < (const Item& i2) const {
      return key < i2.key;
    }
  };
  using value_type = DataType;

  SortArrBI(size_t max_elem) :  v_(max_elem) {
    if (max_elem <= 0) {
      throw std::runtime_error("The maximum number of elements in MinHeapPseudoBI should be > 0");
    }
  }

  // resize may invalidate the reference returned by get_data() !!!!
  void resize(size_t max_elem) {
    v_.resize(max_elem);
  }

  // push_unsorted_grow may invalidate the reference returned by get_data() !!!!
  void push_unsorted_grow(const KeyType& key, const DataType& data) {
    if (num_elems_ + 1 >= v_.size()) resize(num_elems_+1);
    v_[num_elems_].used = false;
    v_[num_elems_].key = key;
    v_[num_elems_].data = data;
    num_elems_++;
  }

  KeyType top_key() {
    return v_[num_elems_-1].key;
  }

  const Item& top_item() const {
    return v_[num_elems_-1];
  }

  void sort() {
    if (!v_.empty())
      PREFETCH(&v_[0], _MM_HINT_T0);
    std::sort(v_.begin(), v_.begin() + num_elems_);
  }

  void swap(size_t x, size_t y) {
    std::swap(v_[x], v_[y]);
  }

  // Checking for duplicate IDs isn't the responsibility of this function
  // it also assumes a non-empty array
  size_t push_or_replace_non_empty(const KeyType& key, const DataType& data) {
    // num_elems_ > 0
    size_t curr = num_elems_ - 1;
    if (v_[curr].key <= key) {
      if (num_elems_ < v_.size()) {
        v_[num_elems_].used = false;
        v_[num_elems_].key  = key;
        v_[num_elems_].data = data;
        return num_elems_++;
      } else {
        return num_elems_;
      }
    }

    while (curr > 0) {
      size_t j = curr - 1;
      if (v_[j].key <= key) break;
      curr = j;
    }

    if (num_elems_ < v_.size()) num_elems_++;
    // curr + 1 <= num_elems_
    PREFETCH((char *)&v_[curr], _MM_HINT_T0);

    if (num_elems_ - (1 + curr) > 0)
      memmove((char *)&v_[curr+1], &v_[curr], (num_elems_ - (1 + curr)) * sizeof(v_[0]));
    
    v_[curr].used = false;
    v_[curr].key  = key;
    v_[curr].data = data;
    return curr;
  }

  // In-place merge
  size_t merge_with_sorted_items(Item* items, size_t item_qty) {
    if (!item_qty) return num_elems_;

    if (item_qty > v_.size()) item_qty = v_.size();
    //if (item_qty == 1) return push_or_replace_non_empty_exp(items[0].key, items[0].data);

    const size_t left_qty = v_.size() - num_elems_;

    if (left_qty >= item_qty) {
      memcpy(&v_[num_elems_], items, item_qty * sizeof(Item));

      std::inplace_merge(v_.begin(), v_.begin() + num_elems_, v_.begin() + num_elems_ + item_qty);
      num_elems_ += item_qty;

    } else {
      // Here left_qty < item_qty
      size_t remove_qty = 0;
      while (item_qty    > left_qty+ remove_qty &&
             num_elems_  > remove_qty           && // This entails that num_elems_ - remove_qty - 1 >= 0
             items[left_qty + remove_qty].key < v_[num_elems_  - remove_qty - 1].key) {
        remove_qty++;
      }
      memcpy(&v_[num_elems_- remove_qty], items, (left_qty + remove_qty)* sizeof(Item));

      // Note that num_elems_ + left_qty == v_.size()
      std::inplace_merge(v_.begin(), v_.begin() + num_elems_- remove_qty, v_.end());
      num_elems_ = v_.size(); // filling out the buffer completely
    }

    size_t ret = 0;
    while (ret < num_elems_ && v_[ret].used) ++ret;
    return ret;
  }

  // Checking for duplicate IDs isn't the responsibility of this function
  // it also assumes a non-empty array
  size_t push_or_replace_non_empty_exp(const KeyType& key, const DataType& data) {
    // num_elems_ > 0
    size_t curr = num_elems_ - 1;
    if (v_[curr].key <= key) {
      if (num_elems_ < v_.size()) {
        v_[num_elems_].used = false;
        v_[num_elems_].key  = key;
        v_[num_elems_].data = data;
        return num_elems_++;
      } else {
        return num_elems_;
      }
    }
    size_t prev = curr;

    size_t d=1;
    // always curr >= d
    while (curr > 0 && v_[curr].key > key) {
      prev = curr;
      curr -= d;
      d *= 2;
      if (d > curr) d = curr;
    }

    PREFETCH((char*)&v_[curr], _MM_HINT_T0);
    if (curr < prev) {
      curr = std::lower_bound(&v_[curr], &v_[prev], Item(key)) - &v_[0];
    }

    if (num_elems_ < v_.size()) num_elems_++;
    // curr + 1 <= num_elems_

    if (num_elems_ - (1 + curr) > 0)
		   memmove(&v_[curr+1], &v_[curr], (num_elems_ - (1 + curr)) * sizeof(v_[0]));


    v_[curr].used = false;
    v_[curr].key  = key;
    v_[curr].data = data;
    return curr;
  }


  const std::vector<Item>& get_data() const {
    return this->v_;
  }

  std::vector<Item>& get_data() {
    return this->v_;
  }

  size_t size() const { return num_elems_; }
 
 protected:

  std::vector<Item> v_;
  size_t num_elems_ = 0;
};


#endif
