#include <library/cpp/threading/light_rw_lock/lightrwlock.h>
#include <util/random/random.h>

#ifdef _linux_
// Light rw lock is implemented only for linux

using namespace NS_LightRWLock;

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#define LIGHT

#ifdef RWSPINLOCK
#include <library/cpp/lwtrace/rwspinlock.h>
#endif

#define CHECK_LOGIC 1
#define LOOPCOUNT 1000000
#define RANRCOUNT 100
#define THREADCOUNT 40
#define WRITELOCKS 100

#if defined(_MSC_VER)
static int Y_FORCE_INLINE AtomicFetchAdd(volatile int& item, int value) {
    return _InterlockedExchangeAdd((&item, value);
}
#elif defined(__GNUC__)
#else
#error unsupported platform
#endif

class TPosixRWLock {
public:
    TPosixRWLock() {
    }

    ~TPosixRWLock() {
        pthread_rwlock_destroy(&rwlock);
    }

    TPosixRWLock(const TPosixRWLock&) = delete;
    void operator=(const TPosixRWLock&) = delete;

private:
    pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;
    friend class TPosixRWShareLocker;
    friend class TPosixRWExclusiveLocker;
};

#if defined(LIGHT)
TLightRWLock __attribute__((aligned(64))) rwlock;
#elif defined(POSIX)
TPosixRWLock rwlock;
#elif defined(RWSPINLOCK)
TRWSpinLock __attribute__((aligned(64))) rwlock;
#else
#error "define lock type"
#endif

volatile __attribute__((aligned(64))) int checkIt = 0;
volatile int checkExcl = 0;

class TPosixRWShareLocker {
public:
    TPosixRWShareLocker(TPosixRWLock& lock)
        : LockP_(&lock)
    {
        pthread_rwlock_rdlock(&LockP_->rwlock);
    }

    ~TPosixRWShareLocker() {
        pthread_rwlock_unlock(&LockP_->rwlock);
    }

    TPosixRWShareLocker(const TPosixRWShareLocker&) = delete;
    void operator=(const TPosixRWShareLocker&) = delete;

private:
    TPosixRWLock* LockP_;
};

class TPosixRWExclusiveLocker {
public:
    TPosixRWExclusiveLocker(TPosixRWLock& lock)
        : LockP_(&lock)
    {
        pthread_rwlock_wrlock(&LockP_->rwlock);
    }

    ~TPosixRWExclusiveLocker() {
        pthread_rwlock_unlock(&LockP_->rwlock);
    }
    TPosixRWExclusiveLocker(const TPosixRWExclusiveLocker&) = delete;
    void operator=(const TPosixRWExclusiveLocker&) = delete;

private:
    TPosixRWLock* LockP_;
};

template <typename TLocker, bool excl>
static Y_FORCE_INLINE void Run() {
    TLocker lockIt(rwlock);

#if defined(CHECK_LOGIC) && CHECK_LOGIC
    if (!excl && checkExcl == 1) {
        printf("there is a bug\n");
    }

    int result = AtomicFetchAdd(checkIt, 1);
    if (excl)
        checkExcl = 1;

    if (excl && result > 1)
        printf("there is a bug\n");
#endif

    for (unsigned w = 0; w < RANRCOUNT; ++w)
        RandomNumber<ui32>();

#if defined(CHECK_LOGIC) && CHECK_LOGIC
    if (excl)
        checkExcl = 0;

    AtomicFetchAdd(checkIt, -1);
#endif
}

#ifdef LIGHT
static void* fast_thread_start(__attribute__((unused)) void* arg) {
    for (unsigned q = 0; q < LOOPCOUNT; ++q) {
        char excl = (RandomNumber<ui32>() % WRITELOCKS) == 0;
        if (excl)
            Run<TLightWriteGuard, 1>();
        else
            Run<TLightReadGuard, 0>();
    }
    return NULL;
}
#endif

#ifdef POSIX
static void* fast_thread_start(__attribute__((unused)) void* arg) {
    for (unsigned q = 0; q < LOOPCOUNT; ++q) {
        char excl = (RandomNumber<ui32>() % WRITELOCKS) == 0;
        if (excl)
            Run<TPosixRWExclusiveLocker, 1>();
        else
            Run<TPosixRWShareLocker, 0>();
    }
    return NULL;
}
#endif

#ifdef RWSPINLOCK
static void* fast_thread_start(__attribute__((unused)) void* arg) {
    for (unsigned q = 0; q < LOOPCOUNT; ++q) {
        char excl = (RandomNumber<ui32>() % WRITELOCKS) == 0;
        if (excl)
            Run<TWriteSpinLockGuard, 1>();
        else
            Run<TReadSpinLockGuard, 0>();
    }
    return NULL;
}
#endif

int main() {
    pthread_t threads[THREADCOUNT];

    for (unsigned q = 0; q < THREADCOUNT; ++q) {
        pthread_create(&(threads[q]), NULL, &fast_thread_start, NULL);
    }

    for (unsigned q = 0; q < THREADCOUNT; ++q)
        pthread_join(threads[q], NULL);

    return 0;
}

#else // !_linux_

int main() {
    return 0;
}

#endif
