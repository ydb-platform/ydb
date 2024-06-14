#pragma once
#include <library/cpp/json/writer/json_value.h>

#include <util/system/types.h>
#include <vector>

namespace NKikimr::NArrow::NMerger {

class TRecordBatchBuilder;

template <class TSortCursor>
class TSortingHeap {
private:
    std::deque<TSortCursor> FinishedCursors;
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

    const TSortCursor& Current() const { return Queue.front(); }
    TSortCursor& MutableCurrent() { return Queue.front(); }
    size_t Size() const { return Queue.size(); }
    bool Empty() const { return Queue.empty(); }
    TSortCursor& NextChild() { return Queue[NextChildIndex()]; }

    void Next() {
        Y_ABORT_UNLESS(Size());

        if (Queue.front().Next()) {
            UpdateTop();
        } else {
            RemoveTop();
        }
    }

    void CleanFinished() {
        FinishedCursors.clear();
    }

    void RemoveTop() {
        std::pop_heap(Queue.begin(), Queue.end());
        FinishedCursors.emplace_back(std::move(Queue.back()));
        Queue.pop_back();
        NextIdx = 0;
    }

    void Push(TSortCursor&& cursor) {
        Queue.emplace_back(std::move(cursor));
        std::push_heap(Queue.begin(), Queue.end());
        NextIdx = 0;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_ARRAY;
        for (auto&& i : Queue) {
            result.AppendValue(i.DebugJson());
        }
        return result;
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

            if ((child_idx + 1) < size && *child_it < *(child_it + 1)) {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(*child_it < top));
        *curr_it = std::move(top);
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

};

}
