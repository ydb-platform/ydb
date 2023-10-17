#pragma once

#include <util/generic/vector.h>
#include <util/system/yassert.h>

namespace NKikimr {

/**
 * Intrusive heap that tracks items and supports update/remove operations
 */
template<class T, class THeapIndex, class TCompare = TLess<T>>
class TIntrusiveHeap
    : private THeapIndex
    , private TCompare
{
public:
    T* Top() const {
        return Data ? Data.front() : nullptr;
    }

    bool Has(T* value) {
        size_t index = HeapIndex(value);
        if (index < Data.size() && Data[index] == value) {
            return true;
        }
        return false;
    }

    bool Empty() const {
        return Data.empty();
    }

    size_t Size() const {
        return Data.size();
    }

    explicit operator bool() const {
        return !Empty();
    }

    void Add(T* value) {
        Y_ABORT_UNLESS(HeapIndex(value) == size_t(-1), "Value is already on the heap");
        Data.emplace_back(value);
        HeapIndex(value) = Data.size() - 1;
        SiftUp(value);
    }

    void Remove(T* value) {
        size_t index = std::exchange(HeapIndex(value), size_t(-1));
        Y_ABORT_UNLESS(index != size_t(-1), "Value is not on the heap");
        Y_ABORT_UNLESS(index < Data.size() && Data[index] == value, "Heap index is out of sync");

        for (;;) {
            size_t child = (index << 1) + 1;
            if (!(child < Data.size())) {
                break;
            }

            size_t right = child + 1;
            if (right < Data.size() && Compare(Data[right], Data[child])) {
                child = right; // select the smallest child
            }

            T* cvalue = Data[child];
            Y_DEBUG_ABORT_UNLESS(HeapIndex(cvalue) == child, "Heap index is out of sync");

            // Move the smallest Data[child] up
            Data[index] = cvalue;
            HeapIndex(cvalue) = index;
            index = child;
        }

        if (size_t last = Data.size() - 1; index != last) {
            T* lvalue = Data[last];
            Y_DEBUG_ABORT_UNLESS(HeapIndex(lvalue) == last, "Heap index is out of sync");
            // Move the last item to the vacant slot
            Data[index] = lvalue;
            HeapIndex(lvalue) = index;
            SiftUp(lvalue);
        }

        Data.pop_back();
    }

    bool SiftDown(T* value) {
        bool moved = false;
        size_t index = HeapIndex(value);
        Y_ABORT_UNLESS(index != size_t(-1), "Value is not on the heap");
        Y_ABORT_UNLESS(index < Data.size() && Data[index] == value, "Heap index is out of sync");

        for (;;) {
            size_t child = (index << 1) + 1;
            if (!(child < Data.size())) {
                break;
            }

            size_t right = child + 1;
            if (right < Data.size() && Compare(Data[right], Data[child])) {
                child = right; // select the smallest child
            }

            T* cvalue = Data[child];
            Y_DEBUG_ABORT_UNLESS(HeapIndex(cvalue) == child, "Heap index is out of sync");

            if (!Compare(cvalue, value)) {
                break; // already correct
            }

            // Move the smaller Data[child] up
            Data[index] = cvalue;
            HeapIndex(cvalue) = index;
            index = child;
            moved = true;
        }

        if (moved) {
            Data[index] = value;
            HeapIndex(value) = index;
        }

        return moved;
    }

    bool SiftUp(T* value) {
        bool moved = false;
        size_t index = HeapIndex(value);
        Y_ABORT_UNLESS(index != size_t(-1), "Value is not on the heap");
        Y_ABORT_UNLESS(index < Data.size() && Data[index] == value, "Heap index is out of sync");

        while (index > 0) {
            size_t parent = (index - 1) >> 1;
            T* pvalue = Data[parent];
            Y_DEBUG_ABORT_UNLESS(HeapIndex(pvalue) == parent, "Heap index is out of sync");

            if (!Compare(value, pvalue)) {
                break; // already correct
            }

            // Move the larger Data[parent] down
            Data[index] = pvalue;
            HeapIndex(pvalue) = index;
            index = parent;
            moved = true;
        }

        if (moved) {
            Data[index] = value;
            HeapIndex(value) = index;
        }

        return moved;
    }

    bool Update(T* value) {
        return SiftDown(value) || SiftUp(value);
    }

    void Clear() {
        for (auto& x: Data) {
            HeapIndex(x) = size_t(-1);
        }
        Data.clear();
    }

private:
    inline size_t& HeapIndex(T* value) const {
        return static_cast<const THeapIndex&>(*this)(*value);
    }

    inline bool Compare(const T* a, const T* b) const {
        return static_cast<const TCompare&>(*this)(*a, *b);
    }

private:
    TVector<T*> Data;
};

}   // namespace NKikimr
