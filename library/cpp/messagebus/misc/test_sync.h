#pragma once

#include <util/system/condvar.h>
#include <util/system/mutex.h>

class TTestSync {
private:
    unsigned Current;

    TMutex Mutex;
    TCondVar CondVar;

public:
    TTestSync()
        : Current(0)
    {
    }

    void Inc() {
        TGuard<TMutex> guard(Mutex);

        DoInc();
        CondVar.BroadCast();
    }

    unsigned Get() {
        TGuard<TMutex> guard(Mutex);

        return Current;
    }

    void WaitFor(unsigned n) {
        TGuard<TMutex> guard(Mutex);

        Y_ABORT_UNLESS(Current <= n, "too late, waiting for %d, already %d", n, Current);

        while (n > Current) {
            CondVar.WaitI(Mutex);
        }
    }

    void WaitForAndIncrement(unsigned n) {
        TGuard<TMutex> guard(Mutex);

        Y_ABORT_UNLESS(Current <= n, "too late, waiting for %d, already %d", n, Current);

        while (n > Current) {
            CondVar.WaitI(Mutex);
        }

        DoInc();
        CondVar.BroadCast();
    }

    void CheckAndIncrement(unsigned n) {
        TGuard<TMutex> guard(Mutex);

        Y_ABORT_UNLESS(Current == n, "must be %d, currently %d", n, Current);

        DoInc();
        CondVar.BroadCast();
    }

    void Check(unsigned n) {
        TGuard<TMutex> guard(Mutex);

        Y_ABORT_UNLESS(Current == n, "must be %d, currently %d", n, Current);
    }

private:
    void DoInc() {
        unsigned r = ++Current;
        Y_UNUSED(r);
    }
};
