#pragma once

#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/coroutine/engine/mutex.h>
#include <library/cpp/coroutine/engine/network.h>

#include <util/network/pair.h>
#include <util/network/poller.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/yield.h>
#include <util/generic/noncopyable.h>

/*****************************************************************
      TContSemaphore
           ^
           |
      TCoSemaphore
           ^
           |
        TCoMutex
 *****************************************************************/

/*
 * Note: this semaphore does not try to be thread-safe.
 * It is intended to use within coroutines in single thread only.
 * Do not share it between threads. Use TContMtSpinlock instead.
 */

class TContSemaphore {
public:
    inline TContSemaphore(const unsigned int initial)
        : mValue(initial)
    {
    }

    inline bool Acquire(TCont* cont) {
        while (!mValue) {
            mWaitQueue.WaitI(cont);
        }
        --mValue;
        return true;
    }

    inline bool Release(TCont*) {
        ++mValue;
        mWaitQueue.Signal();
        return true;
    }

    inline bool TryAcquire(TCont*) {
        if (!mValue) {
            return false;
        }
        --mValue;
        return true;
    }

protected:
    TAtomic mValue;
    TContWaitQueue mWaitQueue;
};

/*****************************************************************
                        TContMtSyncBase
                         ^          ^
                        /            \
                       /              \
            TContMtSpinlock         TContMtSemaphore
                  ^
                  |
             TCoMtSpinlock
 *****************************************************************/
class TContMtSyncBase {
public:
    static inline void Yield(TCont* cont) {
        //printf("yield() called with cont=%p in thread %p\n", cont, pthread_self()); fflush(stdout);
        if (cont) {
            // Bugfeature workaround: coroutine Yield() in loop would make our priority too high to receive any other events
            // so we probably never come out of this loop. So better use Sleep() instead. Unlike Yield(), Sleep(0)
            // makes all pending I/Os have higher priority than us.
            cont->SleepT(TDuration::Zero());
        } else {
            ThreadYield();
        }
    }

    static inline void SleepT(TCont* cont, unsigned time) {
        if (cont) {
            cont->SleepT(TDuration::MicroSeconds(time));
        } else {
            ::usleep(time);
        }
    }
};

/*
 * Thread-safe version of TContMutex
 */

class TContMtSpinlock: private TContMtSyncBase {
public:
    TContMtSpinlock()
        : mToken(true)
    {
    }

    inline int LockI(TCont* cont) const {
        // We do not want to lock the bus for a long time. However, we do not want to switch context
        // immediately. Hope that 16 times is enough to finish most of the small operations.
        for (int i = 0; i < 4; ++i) {
            if (AtomicSwap(&mToken, false))
                return 0;
        }
        for (int i = 0; i < 8; ++i) {
            if (AtomicSwap(&mToken, false))
                return 0;
            Yield(cont);
        }
        for (int i = 0; i < 4; ++i) {
            if (AtomicSwap(&mToken, false))
                return 0;
            SleepT(cont, 1000);
        }
        while (!AtomicSwap(&mToken, false))
            SleepT(cont, 10000);
        return 0;
    }

    inline bool Acquire(TCont* cont) const {
        return (LockI(cont) == 0);
    }

    inline int LockD(TCont* cont, TInstant deadline) {
        for (int i = 0; i < 4; ++i) {
            if (AtomicSwap(&mToken, false))
                return 0;
        }
        for (int i = 0; i < 8; ++i) {
            if (AtomicSwap(&mToken, false))
                return 0;
            if (Now() > deadline)
                return ETIMEDOUT;
            Yield(cont);
        }
        for (int i = 0; i < 4; ++i) {
            if (AtomicSwap(&mToken, false))
                return 0;
            if (Now() > deadline)
                return ETIMEDOUT;
            SleepT(cont, 1000);
        }
        while (!AtomicSwap(&mToken, false)) {
            if (Now() > deadline)
                return ETIMEDOUT;
            SleepT(cont, 10000);
        }
        return 0;
    }

    inline int LockT(TCont* cont, TDuration timeout) {
        return LockD(cont, Now() + timeout);
    }

    inline void UnLock(TCont*) const {
        mToken = true;
    }

    inline bool Release(TCont* cont) const {
        UnLock(cont);
        return true;
    }

    inline bool TryAcquire(TCont*) const {
        return AtomicSwap(&mToken, false);
    }

private:
    TAtomic mutable mToken;
};

/* Thread-safe version of TContSemaphore */

class TContMtSemaphore: private TContMtSyncBase {
public:
    TContMtSemaphore(const unsigned int initial)
        : mValue(initial)
    {
    }

    inline bool Acquire(TCont* cont) {
        for (int i = 0; i < 1024; ++i) {
            if (TryAcquire(cont))
                return true;
        }
        while (!TryAcquire(cont))
            Yield(cont);
        return true;
    }

    inline bool Release(TCont*) {
        AtomicAdd(mValue, 1);
        return true;
    }

    inline bool TryAcquire(TCont*) {
        TAtomic q = AtomicAdd(mValue, -1);
        if (q < 0) {
            AtomicAdd(mValue, 1); // this is safe even if some other thread accessed mValue
                                  // since increments and decrements are always balanced
            return false;
        }
        return true;
    }

private:
    TAtomic mValue;
};

/*****************************************************************
                            TContSem
 *****************************************************************/
class TContSem {
public:
    TContSem(void*) noexcept {
        pfd[0] = pfd[1] = INVALID_SOCKET;
        val = 0;
    }

    ~TContSem() {
        if (pfd[0] != INVALID_SOCKET)
            closesocket(pfd[0]);
        if (pfd[1] != INVALID_SOCKET)
            closesocket(pfd[1]);
        pfd[0] = pfd[1] = INVALID_SOCKET;
        val = 0;
    }

    bool Initialized() noexcept {
        return pfd[0] != INVALID_SOCKET;
    }

    int Init(unsigned short Val) noexcept {
        assert(pfd[0] == INVALID_SOCKET && pfd[1] == INVALID_SOCKET);

        if (SocketPair(pfd) == SOCKET_ERROR) {
            return errno;
        }

        char ch = 0;
        val = Val;

        for (unsigned short i = 0; i < val; i++)
            if (send(pfd[1], &ch, 1, 0) != 1) {
                int err = errno;
                closesocket(pfd[0]);
                closesocket(pfd[1]);
                pfd[0] = pfd[1] = INVALID_SOCKET;
                return err;
            }
#if defined(_unix_)
        if (fcntl(pfd[0], F_SETFL, O_NONBLOCK) == -1)
            return errno;
        if (fcntl(pfd[1], F_SETFL, O_NONBLOCK) == -1)
            return errno;
#endif

        readPoller.WaitRead(pfd[0], this);
        writePoller.WaitWrite(pfd[1], this);

        return 0;
    }

    int DownD(TCont* cont, TInstant deadline) {
        assert(pfd[0] != INVALID_SOCKET && pfd[1] != INVALID_SOCKET);
        assert(cont);
        --val;

        char ch = 0;
        int result = downMutex.LockI(cont);
        if (result)
            return result;
        TContIOStatus status = NCoro::ReadD(cont, pfd[0], &ch, 1, deadline);
        downMutex.UnLock();
        if (status.Status())
            return status.Status();
        return 0;
    }

    int DownT(TCont* cont, TDuration timeout) {
        return DownD(cont, Now() + timeout);
    }

    int DownI(TCont* cont) {
        return DownD(cont, TInstant::Max());
    }

    int Down() {
        assert(pfd[0] != INVALID_SOCKET && pfd[1] != INVALID_SOCKET);
        --val;

        char ch = 0;
        while (true) {
            int result = recv(pfd[0], &ch, 1, 0);
            if (result == INVALID_SOCKET) {
                if (errno != EINTR && errno != EAGAIN) {
                    return errno;
                }
            } else {
                break;
            }
            void* pollerResult = readPoller.WaitI();
            Y_ASSERT(pollerResult == this);
        }
        return 0;
    }

    int Up(TCont* cont) {
        assert(pfd[0] != INVALID_SOCKET && pfd[1] != INVALID_SOCKET);
        assert(cont);
        ++val;

        char ch = 0;
        upMutex.LockI(cont);
        TContIOStatus status = NCoro::WriteI(cont, pfd[1], &ch, 1);
        upMutex.UnLock();
        if (status.Status())
            return status.Status();
        return 0;
    }

    int Up() {
        assert(pfd[0] != INVALID_SOCKET && pfd[1] != INVALID_SOCKET);
        ++val;

        char ch = 0;
        while (true) {
            int result = send(pfd[1], &ch, 1, 0);
            if (result == INVALID_SOCKET) {
                if (errno != EINTR && errno != EAGAIN) {
                    return errno;
                }
            } else {
                break;
            }
            void* pollerResult = writePoller.WaitI();
            Y_ASSERT(pollerResult == this);
        }
        return 0;
    }

private:
    SOCKET pfd[2];
    unsigned short val;
    TSocketPoller readPoller;
    TSocketPoller writePoller;
    TContMutex downMutex;
    TContMutex upMutex;

    // forbid copying
    TContSem(const TContSem&) {
    }
    TContSem& operator=(const TContSem&) {
        return *this;
    }
};

class TCoMtMutex {
public:
    TCoMtMutex()
        : Mutex(this)
    {
        Mutex.Init(1);
    }

    void Acquire() {
        Mutex.Down();
    }

    void Acquire(TCont* c) {
        Mutex.DownI(c);
    }

    void Release(TCont* c) {
        Mutex.Up(c);
    }

    void Release() {
        Mutex.Up();
    }

private:
    TContSem Mutex;
};

/*****************************************************************
                            GUARDS
 *****************************************************************/
class SemaphoreGuard : TNonCopyable {
public:
    inline SemaphoreGuard(TCont* cont, TContSemaphore& semaphore)
        : mSemaphore(semaphore)
        , mCont(cont)
    {
        mSemaphore.Acquire(mCont);
    }

    inline ~SemaphoreGuard() {
        mSemaphore.Release(mCont);
    }

private:
    TContSemaphore& mSemaphore;
    TCont* mCont;
};

class ContGuard {
public:
    inline ContGuard(TCont* cont, TContMtSpinlock& lock)
        : mLock(lock)
        , mCont(cont)
    {
        mLock.LockI(mCont);
    }

    inline ~ContGuard() {
        mLock.UnLock(mCont);
    }

private:
    TContMtSpinlock& mLock;
    TCont* mCont;
};
