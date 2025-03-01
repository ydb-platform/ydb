/*******************************************************************************
 * tlx/container/d_ary_heap.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Eugenio Angriman <angrimae@hu-berlin.de>
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_D_ARY_HEAP_HEADER
#define TLX_CONTAINER_D_ARY_HEAP_HEADER

#include <cassert>
#include <cstddef>
#include <functional>
#include <limits>
#include <queue>
#include <vector>

namespace tlx {

//! \addtogroup tlx_container
//! \{

/*!
 * This class implements a d-ary comparison-based heap usable as a priority
 * queue. Higher arity yields better cache efficiency.
 *
 * \tparam KeyType    Key type.
 * \tparam Arity      A positive integer.
 * \tparam Compare    Function object to order keys.
 */
template <typename KeyType, unsigned Arity = 2,
          typename Compare = std::less<KeyType> >
class DAryHeap
{
    static_assert(Arity, "Arity must be greater than zero.");

public:
    using key_type = KeyType;
    using compare_type = Compare;

    static constexpr size_t arity = Arity;

protected:
    //! Cells in the heap.
    std::vector<key_type> heap_;

    //! Compare function.
    compare_type cmp_;

public:
    //! Allocates an empty heap.
    explicit DAryHeap(compare_type cmp = compare_type())
        : heap_(0), cmp_(cmp) { }

    //! Allocates space for \c new_size items.
    void reserve(size_t new_size) {
        heap_.reserve(new_size);
    }

    //! Copy.
    DAryHeap(const DAryHeap&) = default;
    DAryHeap& operator = (const DAryHeap&) = default;

    //! Move.
    DAryHeap(DAryHeap&&) = default;
    DAryHeap& operator = (DAryHeap&&) = default;

    //! Empties the heap.
    void clear() {
        heap_.clear();
    }

    //! Returns the number of items in the heap.
    size_t size() const noexcept { return heap_.size(); }

    //! Returns the capacity of the heap.
    size_t capacity() const noexcept { return heap_.capacity(); }

    //! Returns true if the heap has no items, false otherwise.
    bool empty() const noexcept { return heap_.empty(); }

    //! Inserts a new item.
    void push(const key_type& new_key) {
        // Insert the new item at the end of the heap.
        heap_.push_back(new_key);
        sift_up(heap_.size() - 1);
    }

    //! Inserts a new item.
    void push(key_type&& new_key) {
        // Insert the new item at the end of the heap.
        heap_.push_back(std::move(new_key));
        sift_up(heap_.size() - 1);
    }

    //! Returns the top item.
    const key_type& top() const noexcept {
        assert(!empty());
        return heap_[0];
    }

    //! Removes the top item.
    void pop() {
        assert(!empty());
        std::swap(heap_[0], heap_.back());
        heap_.pop_back();
        if (!heap_.empty())
            sift_down(0);
    }

    //! Removes and returns the top item.
    key_type extract_top() {
        key_type top_item = top();
        pop();
        return top_item;
    }

    //! Rebuilds the heap.
    void update_all() {
        heapify();
    }

    //! Builds a heap from a container.
    template <class InputIterator>
    void build_heap(InputIterator first, InputIterator last) {
        heap_.assign(first, last);
        heapify();
    }

    //! Builds a heap from the vector \c keys. Items of \c keys are copied.
    void build_heap(const std::vector<key_type>& keys) {
        heap_.resize(keys.size());
        std::copy(keys.begin(), keys.end(), heap_.begin());
        heapify();
    }

    //! Builds a heap from the vector \c keys. Items of \c keys are moved.
    void build_heap(std::vector<key_type>&& keys) {
        if (!empty())
            heap_.clear();
        heap_ = std::move(keys);
        heapify();
    }

    //! For debugging: runs a BFS from the root node and verifies that the heap
    //! property is respected.
    bool sanity_check() {
        if (empty()) {
            return true;
        }
        std::queue<size_t> q;
        // Explore from the root.
        q.push(0);
        while (!q.empty()) {
            size_t s = q.front();
            q.pop();
            size_t l = left(s);
            for (size_t i = 0; i < arity && l < heap_.size(); ++i) {
                // Check that the priority of the children is not strictly less
                // than their parent.
                if (cmp_(heap_[l], heap_[s]))
                    return false;
                q.push(l++);
            }
        }
        return true;
    }

private:
    //! Returns the position of the left child of the node at position \c k.
    size_t left(size_t k) const { return arity * k + 1; }

    //! Returns the position of the parent of the node at position \c k.
    size_t parent(size_t k) const { return (k - 1) / arity; }

    //! Pushes the node at position \c k up until either it becomes the root or
    //! its parent has lower or equal priority.
    void sift_up(size_t k) {
        key_type value = std::move(heap_[k]);
        size_t p = parent(k);
        while (k > 0 && !cmp_(heap_[p], value)) {
            heap_[k] = std::move(heap_[p]);
            k = p, p = parent(k);
        }
        heap_[k] = std::move(value);
    }

    //! Pushes the item at position \c k down until either it becomes a leaf or
    //! all its children have higher priority
    void sift_down(size_t k) {
        key_type value = std::move(heap_[k]);
        while (true) {
            size_t l = left(k);
            if (l >= heap_.size()) {
                break;
            }
            // Get the min child.
            size_t c = l;
            size_t right = std::min(heap_.size(), c + arity);
            while (++l < right) {
                if (cmp_(heap_[l], heap_[c])) {
                    c = l;
                }
            }

            // Current item has lower or equal priority than the child with
            // minimum priority, stop.
            if (!cmp_(heap_[c], value)) {
                break;
            }

            // Swap current item with the child with minimum priority.
            heap_[k] = std::move(heap_[c]);
            k = c;
        }
        heap_[k] = std::move(value);
    }

    //! Reorganize heap_ into a heap.
    void heapify() {
        if (heap_.size() >= 2) {
            // Iterate from the last internal node up to the root.
            size_t last_internal = (heap_.size() - 2) / arity;
            for (size_t i = last_internal + 1; i; --i) {
                // Index of the current internal node.
                size_t cur = i - 1;
                key_type value = std::move(heap_[cur]);

                do {
                    size_t l = left(cur);
                    // Find the minimum child of cur.
                    size_t min_elem = l;
                    for (size_t j = l + 1;
                         j - l < arity && j < heap_.size(); ++j) {
                        if (cmp_(heap_[j], heap_[min_elem]))
                            min_elem = j;
                    }

                    // One of the children of cur is less then cur: swap and
                    // do another iteration.
                    if (cmp_(heap_[min_elem], value)) {
                        heap_[cur] = std::move(heap_[min_elem]);
                        cur = min_elem;
                    }
                    else
                        break;
                } while (cur <= last_internal);
                heap_[cur] = std::move(value);
            }
        }
    }
};

//! make template alias due to similarity with std::priority_queue
template <typename KeyType, unsigned Arity = 2,
          typename Compare = std::less<KeyType> >
using d_ary_heap = DAryHeap<KeyType, Arity, Compare>;

//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_D_ARY_HEAP_HEADER

/******************************************************************************/
