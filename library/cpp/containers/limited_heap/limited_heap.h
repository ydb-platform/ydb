#pragma once

#include <util/generic/queue.h>
#include <util/generic/algorithm.h>

template <class T, class TComparator = TGreater<T>>
class TLimitedHeap {
private:
    size_t MaxSize;
    TComparator Comparer;
    TPriorityQueue<T, TVector<T>, TComparator> Internal;

public:
    TLimitedHeap(size_t maxSize, const TComparator& comp = TComparator())
        : MaxSize(maxSize)
        , Comparer(comp)
        , Internal(Comparer)
    {
        Y_ASSERT(maxSize >= 1);
    }

    const T& GetMin() const {
        return Internal.top();
    }

    void PopMin() {
        Internal.pop();
    }

    bool Insert(const T& value) {
        if (GetSize() == MaxSize) {
            if (Comparer(GetMin(), value)) {
                return false;
            } else {
                PopMin();
            }
        }

        Internal.push(value);

        return true;
    }

    bool IsEmpty() const {
        return Internal.empty();
    }

    size_t GetSize() const {
        return Internal.size();
    }

    size_t GetMaxSize() const {
        return MaxSize;
    }

    void SetMaxSize(size_t newMaxSize) {
        while (GetSize() > newMaxSize) {
            PopMin();
        }
        MaxSize = newMaxSize;
    }
};
