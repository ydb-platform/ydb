#pragma once

#include <atomic>

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
    std::atomic_int State;

    void Init() noexcept {
        State = 0;
    }

    void AcquireRead() noexcept {
        while (true) {
            int a = State.load();
            if ((a & 1) == 0 && State.compare_exchange_strong(a, a + 2)) {
                break;
            }
            SpinLockPause();
        }
    }

    void ReleaseRead() noexcept {
        State.fetch_add(-2);
    }

    void AcquireWrite() noexcept {
        while (true) {
            int a = State.load();
            if ((a & 1) == 0 && State.compare_exchange_strong(a, a + 1)) {
                break;
            }
            SpinLockPause();
        }

<<<<<<< HEAD
        while (!AtomicCas(&State, TAtomicBase(-1), 1)) {
            SpinLockPause();
=======
        while (true) {
            int a = 1;
            if (State.compare_exchange_strong(a, -1)) {
                break;
            }
            sw.Sleep();
>>>>>>> f7a16641f3f... use C++ atomic with proper default constructor
        }
    }

    void ReleaseWrite() noexcept {
        State.store(0);
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
