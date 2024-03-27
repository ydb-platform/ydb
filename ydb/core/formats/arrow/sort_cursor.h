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
        auto rbSorting = ExtractColumns(batch, desc->SortingKey);
        Y_ABORT_UNLESS(rbSorting);
        sort_columns = std::make_shared<TArrayVec>(rbSorting->columns());
        all_columns = &batch->columns();
        if (desc->ReplaceKey) {
            auto rbReplace = ExtractColumns(batch, desc->ReplaceKey);
            Y_ABORT_UNLESS(rbReplace);
            replace_columns = std::make_shared<TArrayVec>(rbReplace->columns());
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

        if (NotNull) {
            for (size_t i = 0; i < Impl->desc->Size(); ++i) {
                auto cmp = left.CompareColumnValueNotNull(i, right, i);
                int res = Impl->desc->Direction(i) * (std::is_eq(cmp) ? 0 : (std::is_lt(cmp) ? -1 : 1));
                if (res > 0)
                    return true;
                if (res < 0)
                    return false;
            }
        } else {
            for (size_t i = 0; i < Impl->desc->Size(); ++i) {
                auto cmp = left.CompareColumnValue(i, right, i);
                int res = Impl->desc->Direction(i) * (std::is_eq(cmp) ? 0 : (std::is_lt(cmp) ? -1 : 1));
                if (res > 0)
                    return true;
                if (res < 0)
                    return false;
            }
        }
        return Impl->order > rhs.Impl->order;
    }
};


/// Allows to fetch data from multiple sort cursors in sorted order (merging sorted data stream).
/// Replaced with "Loser Tree", see https://en.wikipedia.org/wiki/K-way_merge_algorithm
class TLoserTree {
public:
    TLoserTree() = default;

    template <typename TCursors>
    TLoserTree(TCursors& tCursors, bool notNull) {
        Cursors.reserve(tCursors.size());
        for (auto & cur : tCursors)
            if (!cur.Empty())
                Cursors.emplace_back(TSortCursor(&cur, notNull));

        while (DataSize < Cursors.size())
            DataSize *= 2;

        Tree.resize(DataSize, -1);
        Tree.shrink_to_fit();
        Build(0);
    }

    bool IsValid() const { return Tree[0] != -1; }
    TSortCursor& Current() { return Cursors[Tree[0]]; }

    void Next() {
        Y_ABORT_UNLESS(IsValid());

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
        int32_t top = Tree[0];
        Tree[0] = -1;
        Update(top, -1);
    }

private:
    std::vector<TSortCursor> Cursors;
    std::vector<int32_t> Tree;

    size_t DataSize{1};

    int32_t Build(size_t v) {
        if (v == 0) {
            Tree[v] = Build(1);
            return 0;
        }
        if (v >= DataSize) {
            v -= DataSize;
            if (v >= Cursors.size())
                return -1;
            return v;
        }
        int32_t left = Build(2 * v);
        int32_t right = Build(2 * v + 1);
        if (left == -1) {
            Tree[v] = left;
            return right;
        }
        if (right == -1) {
            Tree[v] = right;
            return left;
        }
        if (Cursors[right] < Cursors[left]) {
            Tree[v] = right;
            return left;
        }
        else {
            Tree[v] = left;
            return right;
        }
    }

    void UpdateTop() {
        if (Tree[0] != -1)
            Update(Tree[0], Tree[0]);
    }

    void Update(size_t index, int32_t value) {
        for (index += DataSize; index != 1;) {
            size_t parent = index / 2;
            if (value == -1) {
                std::swap(Tree[parent], value);
            }
            else if (Tree[parent] == -1) {
                // Do nothing
            }
            else {
                if (Cursors[value] < Cursors[Tree[parent]])
                    std::swap(Tree[parent], value);
            }
            index = parent;
        }
        Tree[0] = value;
    }
};

}
