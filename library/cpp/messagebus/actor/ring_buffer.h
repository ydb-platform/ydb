#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/maybe.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

template <typename T>
struct TRingBuffer {
private:
    ui32 CapacityPow;
    ui32 CapacityMask;
    ui32 Capacity;
    ui32 WritePos;
    ui32 ReadPos;
    TVector<T> Data;

    void StateCheck() const {
        Y_ASSERT(Capacity == Data.size());
        Y_ASSERT(Capacity == (1u << CapacityPow));
        Y_ASSERT((Capacity & CapacityMask) == 0u);
        Y_ASSERT(Capacity - CapacityMask == 1u);
        Y_ASSERT(WritePos < Capacity);
        Y_ASSERT(ReadPos < Capacity);
    }

    size_t Writable() const {
        return (Capacity + ReadPos - WritePos - 1) & CapacityMask;
    }

    void ReserveWritable(ui32 sz) {
        if (sz <= Writable())
            return;

        ui32 newCapacityPow = CapacityPow;
        while ((1u << newCapacityPow) < sz + ui32(Size()) + 1u) {
            ++newCapacityPow;
        }
        ui32 newCapacity = 1u << newCapacityPow;
        ui32 newCapacityMask = newCapacity - 1u;
        TVector<T> newData(newCapacity);
        ui32 oldSize = Size();
        // Copy old elements
        for (size_t i = 0; i < oldSize; ++i) {
            newData[i] = Get(i);
        }

        CapacityPow = newCapacityPow;
        Capacity = newCapacity;
        CapacityMask = newCapacityMask;
        Data.swap(newData);
        ReadPos = 0;
        WritePos = oldSize;

        StateCheck();
    }

    const T& Get(ui32 i) const {
        return Data[(ReadPos + i) & CapacityMask];
    }

public:
    TRingBuffer()
        : CapacityPow(0)
        , CapacityMask(0)
        , Capacity(1 << CapacityPow)
        , WritePos(0)
        , ReadPos(0)
        , Data(Capacity)
    {
        StateCheck();
    }

    size_t Size() const {
        return (Capacity + WritePos - ReadPos) & CapacityMask;
    }

    bool Empty() const {
        return WritePos == ReadPos;
    }

    void PushAll(TArrayRef<const T> value) {
        ReserveWritable(value.size());

        ui32 secondSize;
        ui32 firstSize;

        if (WritePos + value.size() <= Capacity) {
            firstSize = value.size();
            secondSize = 0;
        } else {
            firstSize = Capacity - WritePos;
            secondSize = value.size() - firstSize;
        }

        for (size_t i = 0; i < firstSize; ++i) {
            Data[WritePos + i] = value[i];
        }

        for (size_t i = 0; i < secondSize; ++i) {
            Data[i] = value[firstSize + i];
        }

        WritePos = (WritePos + value.size()) & CapacityMask;
        StateCheck();
    }

    void Push(const T& t) {
        PushAll(MakeArrayRef(&t, 1));
    }

    bool TryPop(T* r) {
        StateCheck();
        if (Empty()) {
            return false;
        }
        *r = Data[ReadPos];
        ReadPos = (ReadPos + 1) & CapacityMask;
        return true;
    }

    TMaybe<T> TryPop() {
        T tmp;
        if (TryPop(&tmp)) {
            return tmp;
        } else {
            return TMaybe<T>();
        }
    }

    T Pop() {
        return *TryPop();
    }
};
