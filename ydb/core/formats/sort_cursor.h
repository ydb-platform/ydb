// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "replace_key.h"
#include "arrow_helpers.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <algorithm>

namespace NKikimr::NArrow {

/// Description of the sorting rule for several columns.
struct TSortDescription {
    /// @note In case you have PK and snapshot column you should sort with {ASC PK, DESC snap} key and replase with PK
    std::shared_ptr<arrow::Schema> SortingKey;
    std::shared_ptr<arrow::Schema> ReplaceKey; /// Keep first visited (SortingKey ordered) of dups
    std::vector<int> Directions; /// 1 - ascending, -1 - descending.
    bool NotNull{false};
    bool Reverse{false}; // Read sources from bottom to top. With inversed Directions leads to DESC dst for ASC src

    TSortDescription() = default;

    TSortDescription(const std::shared_ptr<arrow::Schema>& sortingKey,
                     const std::shared_ptr<arrow::Schema>& replaceKey = {})
        : SortingKey(sortingKey)
        , ReplaceKey(replaceKey)
        , Directions(sortingKey->num_fields(), 1)
    {}

    size_t Size() const { return SortingKey->num_fields(); }
    int Direction(size_t pos) const { return Directions[pos]; }
    bool Replace() const { return ReplaceKey.get(); }

    void Inverse() {
        Reverse = !Reverse;
        for (int& dir : Directions) {
            dir *= -1;
        }
    }
};


/// Cursor allows to compare rows in different batches.
/// Cursor moves inside single block. It is used in priority queue.
struct TSortCursorImpl {
    std::shared_ptr<TArrayVec> sort_columns;
    std::shared_ptr<TSortDescription> desc;
    ui32 order = 0; // Number of cursor. It determines an order if comparing columns are equal.
    //
    std::shared_ptr<arrow::RecordBatch> current_batch;
    const TArrayVec* all_columns;
    std::shared_ptr<TArrayVec> replace_columns;

    TSortCursorImpl() = default;

    TSortCursorImpl(std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<TSortDescription> desc_, ui32 order_ = 0)
        : desc(desc_)
        , order(order_)
    {
        Reset(batch);
    }

    bool Empty() const { return Rows() == 0; }
    size_t Rows() const { return (!all_columns || all_columns->empty()) ? 0 : all_columns->front()->length(); }
    size_t LastRow() const { return Rows() - 1; }

    void Reset(std::shared_ptr<arrow::RecordBatch> batch) {
        current_batch = batch;
        sort_columns = std::make_shared<TArrayVec>(ExtractColumns(batch, desc->SortingKey)->columns());
        all_columns = &batch->columns();
        if (desc->ReplaceKey) {
            replace_columns = std::make_shared<TArrayVec>(ExtractColumns(batch, desc->ReplaceKey)->columns());
        }
        pos = 0;
    }

    size_t getRow() const {
        return desc->Reverse ? (Rows() - pos - 1) : pos;
    }

    bool isFirst() const { return pos == 0; }
    bool isLast() const { return pos + 1 >= Rows(); }
    bool isValid() const { return pos < Rows(); }
    void next() { ++pos; }

private:
    size_t pos{0};
};


struct TSortCursor {
    TSortCursorImpl* Impl;
    bool NotNull;

    TSortCursor(TSortCursorImpl* impl, bool notNull)
        : Impl(impl)
        , NotNull(notNull)
    {}

    TSortCursorImpl* operator-> () { return Impl; }
    const TSortCursorImpl* operator-> () const { return Impl; }

    bool Greater(const TSortCursor& rhs) const {
        return GreaterAt(rhs, Impl->getRow(), rhs.Impl->getRow());
    }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator < (const TSortCursor& rhs) const {
        return Greater(rhs);
    }

private:
    /// The specified row of this cursor is greater than the specified row of another cursor.
    bool GreaterAt(const TSortCursor& rhs, size_t lhs_pos, size_t rhs_pos) const {
        TRawReplaceKey left(Impl->sort_columns.get(), lhs_pos);
        TRawReplaceKey right(rhs.Impl->sort_columns.get(), rhs_pos);

        for (size_t i = 0; i < Impl->desc->Size(); ++i) {
            int res = Impl->desc->Direction(i) * left.CompareColumnValue(i, right, i, NotNull);
            if (res > 0)
                return true;
            if (res < 0)
                return false;
        }
        return Impl->order > rhs.Impl->order;
    }
};


/// Allows to fetch data from multiple sort cursors in sorted order (merging sorted data stream).
/// TODO: Replace with "Loser Tree", see https://en.wikipedia.org/wiki/K-way_merge_algorithm
class TSortingHeap {
public:
    TSortingHeap() = default;

    template <typename TCursors>
    TSortingHeap(TCursors& cursors, bool notNull) {
        Queue.reserve(cursors.size());
        for (auto& cur : cursors) {
            if (!cur.Empty()) {
                Queue.emplace_back(TSortCursor(&cur, notNull));
            }
        }
        std::make_heap(Queue.begin(), Queue.end());
    }

    bool IsValid() const { return !Queue.empty(); }
    TSortCursor& Current() { return Queue.front(); }
    size_t Size() { return Queue.size(); }
    TSortCursor& NextChild() { return Queue[NextChildIndex()]; }

    void Next() {
        Y_VERIFY(IsValid());

        if (!Current()->isLast()) {
            Current()->next();
            UpdateTop();
        }
        else {
            RemoveTop();
        }
    }

    void ReplaceTop(TSortCursor&& new_top) {
        Current() = new_top;
        UpdateTop();
    }

    void RemoveTop() {
        std::pop_heap(Queue.begin(), Queue.end());
        Queue.pop_back();
        NextIdx = 0;
    }

    void Push(TSortCursor&& cursor) {
        Queue.emplace_back(cursor);
        std::push_heap(Queue.begin(), Queue.end());
        NextIdx = 0;
    }

private:
    std::vector<TSortCursor> Queue;
    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t NextIdx = 0;

    size_t NextChildIndex() {
        if (NextIdx == 0) {
            NextIdx = 1;
            if (Queue.size() > 2 && Queue[1] < Queue[2]) {
                ++NextIdx;
            }
        }

        return NextIdx;
    }

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    /// Also look at "Boost.Heap" library.
    void UpdateTop() {
        size_t size = Queue.size();
        if (size < 2)
            return;

        auto begin = Queue.begin();

        size_t child_idx = NextChildIndex();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (*child_it < *begin)
            return;

        NextIdx = 0;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size)
                break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && *child_it < *(child_it + 1))
            {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(*child_it < top));
        *curr_it = std::move(top);
    }
};

}
