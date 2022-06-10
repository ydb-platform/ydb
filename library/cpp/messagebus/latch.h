#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/condvar.h>
#include <util/system/mutex.h>

class TLatch {
private:
    // 0 for unlocked, 1 for locked
    TAtomic Locked;
    TMutex Mutex;
    TCondVar CondVar;

public:
    TLatch()
        : Locked(0)
    {
    }

    void Wait() {
        // optimistic path
        if (AtomicGet(Locked) == 0) {
            return;
        }

        TGuard<TMutex> guard(Mutex);
        while (AtomicGet(Locked) == 1) {
            CondVar.WaitI(Mutex);
        }
    }

    bool TryWait() {
        return AtomicGet(Locked) == 0;
    }

    void Unlock() {
        // optimistic path
        if (AtomicGet(Locked) == 0) {
            return;
        }

        TGuard<TMutex> guard(Mutex);
        AtomicSet(Locked, 0);
        CondVar.BroadCast();
    }

    void Lock() {
        AtomicSet(Locked, 1);
    }

    bool IsLocked() {
        return AtomicGet(Locked);
    }
};
