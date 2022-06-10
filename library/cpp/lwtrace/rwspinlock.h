#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/spinlock.h>

// State can be one of:
// 0) [NOT_USED] State = 0:
//    * any reader can acquire lock (State += 2: -> READING)
//    * one writer can acquire lock (State = -1: -> WRITING)
// 1) [READING] even number:
//    * State/2 = number of active readers
//    * no writers are waiting
//    * any reader can aqcuire lock (State += 2: -> READING)
//    * active readers can release lock (State -= 2)
//    * one writer can acquire lock (State += 1: -> WAITING)
// 2) [WAITING] odd number > 0:
//    * (State-1)/2 = number of active readers
//    * no more readers/writers can acquire lock
//    * active readers can release lock (State -= 2: -> WAITING)
//    * exactly one writer is waiting for active readers to release lock
//    * if no more active readers left (State == 1) waiting writer acquires lock (State = -1: -> WRITING)
// 3) [WRITING] State = -1
//    * exactly one active writer
//    * no active readers
//    * no more readers/writers can acquire lock
//    * writer can release lock (State = 0: -> READING)

struct TRWSpinLock {
    TAtomic State; // must be initialized by 'TRWSpinLock myLock = {0};' construction

    void Init() noexcept {
        State = 0;
    }

    void AcquireRead() noexcept {
        while (true) {
            TAtomic a = AtomicGet(State);
            if ((a & 1) == 0 && AtomicCas(&State, a + 2, a)) {
                break;
            }
            SpinLockPause();
        }
    }

    void ReleaseRead() noexcept {
        AtomicAdd(State, -2);
    }

    void AcquireWrite() noexcept {
        while (true) {
            TAtomic a = AtomicGet(State);
            if ((a & 1) == 0 && AtomicCas(&State, a + 1, a)) {
                break;
            }
            SpinLockPause();
        }

        while (!AtomicCas(&State, TAtomicBase(-1), 1)) {
            SpinLockPause();
        }
    }

    void ReleaseWrite() noexcept {
        AtomicSet(State, 0);
    }
};

struct TRWSpinLockReadOps {
    static inline void Acquire(TRWSpinLock* t) noexcept {
        t->AcquireRead();
    }

    static inline void Release(TRWSpinLock* t) noexcept {
        t->ReleaseRead();
    }
};

struct TRWSpinLockWriteOps {
    static inline void Acquire(TRWSpinLock* t) noexcept {
        t->AcquireWrite();
    }

    static inline void Release(TRWSpinLock* t) noexcept {
        t->ReleaseWrite();
    }
};

using TReadSpinLockGuard = TGuard<TRWSpinLock, TRWSpinLockReadOps>;
using TWriteSpinLockGuard = TGuard<TRWSpinLock, TRWSpinLockWriteOps>;
