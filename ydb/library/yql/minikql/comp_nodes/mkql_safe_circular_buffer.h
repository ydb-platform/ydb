#pragma once
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/system/yassert.h>
#include <util/generic/maybe.h>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {

template<class T>
class TSafeCircularBuffer {
public:
    TSafeCircularBuffer(TMaybe<size_t> size, T emptyValue, size_t initSize = 0)
        : Buffer(size ? *size : initSize, emptyValue)
        , EmptyValue(emptyValue)
        , Unbounded(!size.Defined())
        , Count(initSize)
    {
        if (!Unbounded) {
            Y_ABORT_UNLESS(initSize <= *size);
        }
    }

    bool IsUnbounded() const {
        return Unbounded;
    }

    void PushBack(T&& data) {
        if (Unbounded) {
            Y_ABORT_UNLESS(Head + Count == Size());
            Buffer.emplace_back(std::move(data));
        } else {
            YQL_ENSURE(!IsFull());
            Buffer[RealIndex(Head + Count)] = std::move(data);
        }
        Count++;
        MutationCount++;
    }

    const T& Get(size_t index) const {
        if (index < Count) {
            return Buffer[RealIndex(Head + index)];
        } else {
            // Circular buffer out of bounds
            return EmptyValue;
        }
    }

    void PopFront() {
        if (!Count) {
            // Circular buffer not have elements for pop, no elements, no problem
        } else {
            Buffer[Head] = EmptyValue;
            Head = RealIndex(Head+1);
            Count--;
        }
        MutationCount++;
    }

    size_t Size() const {
        return Buffer.size();
    }

    size_t UsedSize() const {
        return Count;
    }

    ui64 Generation() const {
        return MutationCount;
    }

    void Clean() {
        const auto usedSize = UsedSize();
        for (size_t index = 0; index < usedSize; ++index) {
            Buffer[RealIndex(Head + index)] = EmptyValue;
        }
    }

    void Clear() {
        Head = Count = 0;
        Buffer.clear();
        Buffer.shrink_to_fit();
    }

private:
    bool IsFull() const {
        if (Unbounded) {
            return false;
        }
        return UsedSize() == Size();
    }

    size_t RealIndex(size_t index) const {
        auto size = Size();
        Y_ABORT_UNLESS(size);
        return index % size;
    }

    std::vector<T> Buffer;
    const T EmptyValue;
    const bool Unbounded;
    size_t Head = 0;
    size_t Count;
    ui64 MutationCount = 0;
};

}
}
