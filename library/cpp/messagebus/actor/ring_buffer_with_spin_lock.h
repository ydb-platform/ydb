#pragma once

#include "ring_buffer.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/spinlock.h>

template <typename T>
class TRingBufferWithSpinLock {
private:
    TRingBuffer<T> RingBuffer;
    TSpinLock SpinLock;
    TAtomic CachedSize;

public:
    TRingBufferWithSpinLock()
        : CachedSize(0)
    {
    }

    void Push(const T& t) {
        PushAll(t);
    }

    void PushAll(TArrayRef<const T> collection) {
        if (collection.empty()) {
            return;
        }

        TGuard<TSpinLock> Guard(SpinLock);
        RingBuffer.PushAll(collection);
        AtomicSet(CachedSize, RingBuffer.Size());
    }

    bool TryPop(T* r, size_t* sizePtr = nullptr) {
        if (AtomicGet(CachedSize) == 0) {
            return false;
        }

        bool ok;
        size_t size;
        {
            TGuard<TSpinLock> Guard(SpinLock);
            ok = RingBuffer.TryPop(r);
            size = RingBuffer.Size();
            AtomicSet(CachedSize, size);
        }
        if (!!sizePtr) {
            *sizePtr = size;
        }
        return ok;
    }

    TMaybe<T> TryPop() {
        T tmp;
        if (TryPop(&tmp)) {
            return tmp;
        } else {
            return TMaybe<T>();
        }
    }

    bool PushAllAndTryPop(TArrayRef<const T> collection, T* r) {
        if (collection.size() == 0) {
            return TryPop(r);
        } else {
            if (AtomicGet(CachedSize) == 0) {
                *r = collection[0];
                if (collection.size() > 1) {
                    TGuard<TSpinLock> guard(SpinLock);
                    RingBuffer.PushAll(MakeArrayRef(collection.data() + 1, collection.size() - 1));
                    AtomicSet(CachedSize, RingBuffer.Size());
                }
            } else {
                TGuard<TSpinLock> guard(SpinLock);
                RingBuffer.PushAll(collection);
                *r = RingBuffer.Pop();
                AtomicSet(CachedSize, RingBuffer.Size());
            }
            return true;
        }
    }

    bool Empty() const {
        return AtomicGet(CachedSize) == 0;
    }

    size_t Size() const {
        TGuard<TSpinLock> Guard(SpinLock);
        return RingBuffer.Size();
    }
};
