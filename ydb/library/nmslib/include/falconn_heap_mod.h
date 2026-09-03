#ifndef  FALCONN_HEAP_MOD_H
#define FALCONN_HEAP_MOD_H

/*
 * A modified MAX-heap priority queue (originally taken from FALCONN https://github.com/FALCONN-LIB/FALCONN).
 * FALCONN licence is MIT, which is Apache 2 compatible
 *
 */

#include <cstdint>
#include <cstdlib>
#include <vector>

/*
 * There are two template MAX-heap modifications here. One uses separate keys and data types,
 * and the other one employs only a user-specified item type (akin to STL's priority_queue.
 *
 * The good things about this priority queue is that it allows a faster update of the top
 * element or the top key. In STL's priority_queue you have to simulate this operation
 * by a combination of push and pop (which can be 2x as long).
 */

template <typename KeyType, typename DataType>
class FalconnHeapMod1 {
 public:
  class Item {
   public:
    KeyType key;
    DataType data;

    Item() {}
    Item(const KeyType& k, const DataType& d) : key(k), data(d) {}

    bool operator < (const Item& i2) const {
      return key < i2.key;
    }
  };
  using value_type = DataType;

  const Item& top_item() const {
    return v_[0];
  }

  const DataType& top_data() const {
    return v_[0].data;
  }

  DataType& top_data() {
    return v_[0].data;
  }

  KeyType top_key() const {
    return this->v_[0].key;
  }

  bool empty() const {
    return num_elements_ == 0;
  }

  void pop() {
    num_elements_ -= 1;
    v_[0] = v_[num_elements_];
    heap_down(0);
  }

  void extract_top(KeyType& key, DataType& data) {
    key  = v_[0].key;
    data = v_[0].data;
    pop();
  }

  void push_unsorted(const KeyType& key, const DataType& data) {
    if (v_.size() == static_cast<size_t>(num_elements_)) {
      v_.emplace_back(Item(key, data));
    } else {
      v_[num_elements_].key = key;
      v_[num_elements_].data = data;
    }
    num_elements_ += 1;
  }

  void push(const KeyType& key, const DataType& data) {
    push_unsorted(key, data);
    heap_up(num_elements_ - 1);
  }

  void heapify() {
    int_fast32_t rightmost = parent(num_elements_ - 1);
    for (int_fast32_t cur_loc = rightmost; cur_loc >= 0; --cur_loc) {
      heap_down(cur_loc);
    }
  }

  void reset() {
    num_elements_ = 0;
  }

  int_fast32_t size() const { return num_elements_; }

  void resize(size_t new_size) {
    v_.resize(new_size);
  }

  void replace_top(const KeyType& key, const DataType& data) {
    this->v_[0].key = key;
    this->v_[0].data = data;
    this->heap_down(0);
  }

  void replace_top_key(const KeyType& key) {
    this->v_[0].key = key;
    this->heap_down(0);
  }

  const std::vector<Item>& get_data() const {
    return this->v_;
  }
 
 protected:
  int_fast32_t lchild(int_fast32_t x) {
    return 2 * x + 1;
  }

  int_fast32_t rchild(int_fast32_t x) {
    return 2 * x + 2;
  }

  int_fast32_t parent(int_fast32_t x) {
    return (x - 1) / 2;
  }

  void swap_entries(int_fast32_t a, int_fast32_t b) {
    Item tmp = v_[a];
    v_[a] = v_[b];
    v_[b] = tmp;
  }

  void heap_up(int_fast32_t cur_loc) {
    int_fast32_t p = parent(cur_loc);
    while (cur_loc > 0 && v_[p].key < v_[cur_loc].key) {
      swap_entries(p, cur_loc);
      cur_loc = p;
      p = parent(cur_loc);
    }
  }

  void heap_down(int_fast32_t cur_loc) {
    while (true) {
      int_fast32_t lc = lchild(cur_loc);
      int_fast32_t rc = rchild(cur_loc);
      if (lc >= num_elements_) {
        return;
      }

      if (v_[cur_loc].key >= v_[lc].key) {
        if (rc >= num_elements_ || v_[cur_loc].key >= v_[rc].key) {
          return;
        } else {
          swap_entries(cur_loc, rc);
          cur_loc = rc;
        }
      } else {
        if (rc >= num_elements_ || v_[lc].key >= v_[rc].key) {
          swap_entries(cur_loc, lc);
          cur_loc = lc;
        } else {
          swap_entries(cur_loc, rc);
          cur_loc = rc;
        }
      }
    }
  }

  std::vector<Item> v_;
  int_fast32_t num_elements_ = 0;
};

template <typename Item>
class FalconnHeapMod2 {
 public:
  using value_type = Item;

  const Item& top() const {
    return v_[0];
  }

  bool empty() const {
    return num_elements_ == 0;
  }

  void pop() {
    num_elements_ -= 1;
    v_[0] = v_[num_elements_];
    heap_down(0);
  }

  void extract_top(Item& item) {
    item  = v_[0];
    pop();
  }

  void push_unsorted(const Item& item) {
    if (v_.size() == static_cast<size_t>(num_elements_)) {
      v_.emplace_back(item);
    } else {
      v_[num_elements_] = item;
    }
    num_elements_ += 1;
  }

  void push(const Item& item) {
    push_unsorted(item);
    heap_up(num_elements_ - 1);
  }

  void heapify() {
    int_fast32_t rightmost = parent(num_elements_ - 1);
    for (int_fast32_t cur_loc = rightmost; cur_loc >= 0; --cur_loc) {
      heap_down(cur_loc);
    }
  }

  void reset() {
    num_elements_ = 0;
  }

  int_fast32_t size() const { return num_elements_; }

  void resize(size_t new_size) {
    v_.resize(new_size);
  }

  void replace_top(const Item& item) {
    this->v_[0] = item;
    this->heap_down(0);
  }

  const std::vector<Item>& get_data() const {
    return this->v_;
  }
 
 protected:
  int_fast32_t lchild(int_fast32_t x) {
    return 2 * x + 1;
  }

  int_fast32_t rchild(int_fast32_t x) {
    return 2 * x + 2;
  }

  int_fast32_t parent(int_fast32_t x) {
    return (x - 1) / 2;
  }

  void swap_entries(int_fast32_t a, int_fast32_t b) {
    Item tmp = v_[a];
    v_[a] = v_[b];
    v_[b] = tmp;
  }

  void heap_up(int_fast32_t cur_loc) {
    int_fast32_t p = parent(cur_loc);
    while (cur_loc > 0 && v_[p] < v_[cur_loc]) {
      swap_entries(p, cur_loc);
      cur_loc = p;
      p = parent(cur_loc);
    }
  }

  void heap_down(int_fast32_t cur_loc) {
    while (true) {
      int_fast32_t lc = lchild(cur_loc);
      int_fast32_t rc = rchild(cur_loc);
      if (lc >= num_elements_) {
        return;
      }

      if (v_[cur_loc] >= v_[lc]) {
        if (rc >= num_elements_ || v_[cur_loc] >= v_[rc]) {
          return;
        } else {
          swap_entries(cur_loc, rc);
          cur_loc = rc;
        }
      } else {
        if (rc >= num_elements_ || v_[lc] >= v_[rc]) {
          swap_entries(cur_loc, lc);
          cur_loc = lc;
        } else {
          swap_entries(cur_loc, rc);
          cur_loc = rc;
        }
      }
    }
  }

  std::vector<Item> v_;
  int_fast32_t num_elements_ = 0;
};


#endif
