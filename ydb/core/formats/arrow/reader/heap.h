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
    static constexpr size_t NoneValue = -1;
public:
    TSortingHeap() = default;

    const TSortCursor& Current() const { return Cursors[Tree.front()]; }
    TSortCursor& MutableCurrent() { return Cursors[Tree.front()]; }
    size_t Size() const { return Capacity() - FreePos.size(); }
    bool Empty() const { return Size() == 0; }

    void Next() {
        Y_ABORT_UNLESS(IsValid());

        if (MutableCurrent().Next()) {
            UpdateTop();
        } else {
            RemoveTop();
        }
    }

    void CleanFinished() {
        FinishedCursors.clear();
    }

    void RemoveTop() {
        Y_ABORT_UNLESS(IsValid());
        size_t old_top = Tree[0];
        Update(old_top, NoneValue);
        FinishedCursors.emplace_back(std::move(Cursors[old_top]));
        FreePos.push_back(old_top);
    }

    void Push(TSortCursor&& cursor) {
        if (FreePos.empty()) {
            IncreaseCap();
        }
        size_t pos = FreePos.back();
        FreePos.pop_back();
        if(pos >= Cursors.size()) {
            Y_ABORT_UNLESS(pos == Cursors.size());
            Cursors.emplace_back(std::move(cursor));
        } else {
            Cursors[pos] = std::move(cursor);
        }
        Update(pos, pos);
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_ARRAY;
        for (size_t i = Tree.size() / 2; i < Tree.size(); ++i) {
            if (Tree[i] != NoneValue) 
                result.AppendValue(Cursors[Tree[i]].DebugJson());
        }
        return result;
    }

    void UpdateTop() {
        if (!Tree.empty() && Tree[0] != NoneValue)
            Update(Tree[0], Tree[0]);
    }

private:
    std::vector<TSortCursor> Cursors;
    std::vector<size_t> Tree;
    std::vector<size_t> FreePos;

    bool IsValid() const { return !Empty(); }

    size_t Capacity() const { return (Tree.size() + 1) / 2; }

    void IncreaseCap() {
        if (Tree.empty()) {
            FreePos.resize(1);
            Cursors.reserve(1);
            Tree.resize(1, NoneValue);
        } else {
            FreePos.reserve(Cursors.size());
            for (size_t i = 0; i < Cursors.size(); ++i) {
                FreePos.push_back(2 * Cursors.size() - 1 - i);
            }
            Cursors.reserve(2 * Cursors.size());

            size_t l = Tree.size() >> 1;
            size_t sz = (Tree.size() + 1) >> 1;

            Tree.resize(Tree.size() * 2 + 1, NoneValue);

            for (; sz != 0; l >>= 1, sz >>= 1) {
                std::copy(Tree.begin() + l, Tree.begin() + l + sz, Tree.begin() + 2 * l + 1);
                std::fill(Tree.begin() + l + sz / 2, Tree.begin() + l + sz, NoneValue);
            }
            Tree[0] = Tree[1];
        }
    }

    void Update(size_t index, size_t value) {
        Y_ABORT_UNLESS(!Tree.empty());

        index += Tree.size() >> 1;
        Tree[index] = value;

        auto cmp = [this](size_t x, size_t y) {
            if (x == NoneValue) {
                return y;
            }
            if (y == NoneValue) {
                return x;
            }
            return Cursors[x] < Cursors[y] ? y : x;
        };

        while (index != 0) {
            size_t parent = (index - 1) >> 1;
            size_t new_val = cmp(Tree[2 * parent + 1], Tree[2 * parent + 2]);
            if (Tree[parent] == new_val && (new_val == NoneValue || new_val != Tree[index])) {
                break;
            }
            Tree[parent] = new_val;
            index = parent;
        }
    }
};
}
