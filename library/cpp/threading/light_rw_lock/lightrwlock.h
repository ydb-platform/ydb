#pragma once

#include <util/system/rwlock.h>
#include <util/system/sanitizers.h>

// TLightRWLock and TSAN are not friends...

#if defined(_linux_) && !defined(_tsan_enabled_)
/* TLightRWLock is optimized for read lock and very fast lock/unlock switching.
   Read lock increments counter.
   Write lock sets highest bit of counter (makes counter negative).

   Whenever a thread tries to acquire read lock that thread increments
   the counter. If the thread gets negative value of the counter right just
   after the increment that means write lock was acquired in another thread.
   In that case the thread decrements the counter back, wakes one thread on
   UnshareFutex, waits on the TrappedFutex and then tries acquire read lock
   from the beginning.
   If the thread gets positive value of the counter after the increment
   then read lock was successfully acquired and
   the thread can proceed execution.

   Whenever a thread tries to acquire write lock that thread set the highest bit
   of the counter. If the thread determine that the bit was set previously then
   write lock was acquired in another thread. In that case the thread waits on
   the TrappedFutex and then tries again from the beginning.
   If the highest bit was successfully set then thread check if any read lock
   exists at the moment. If so the thread waits on UnshareFutex. If there is
   no more read locks then write lock was successfully acquired and the thread
   can proceed execution.
*/

#include <linux/futex.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <errno.h>

namespace NS_LightRWLock {
    static int Y_FORCE_INLINE AtomicFetchAdd(volatile int& item, int value) {
        return __atomic_fetch_add(&item, value, __ATOMIC_SEQ_CST);
    }

#if defined(_x86_64_) || defined(_i386_)

    static char Y_FORCE_INLINE AtomicSetBit(volatile int& item, unsigned bit) {
        char ret;
        __asm__ __volatile__(
            "lock bts %2,%0\n"
            "setc %1\n"
            : "+m"(item), "=rm"(ret)
            : "r"(bit)
            : "cc");

        // msan doesn't treat ret as initialized
        NSan::Unpoison(&ret, sizeof(ret));

        return ret;
    }

    static char Y_FORCE_INLINE AtomicClearBit(volatile int& item, unsigned bit) {
        char ret;
        __asm__ __volatile__(
            "lock btc %2,%0\n"
            "setc %1\n"
            : "+m"(item), "=rm"(ret)
            : "r"(bit)
            : "cc");

        // msan doesn't treat ret as initialized
        NSan::Unpoison(&ret, sizeof(ret));

        return ret;
    }


#else

    static char Y_FORCE_INLINE AtomicSetBit(volatile int& item, unsigned bit) {
        int prev = __atomic_fetch_or(&item, 1 << bit, __ATOMIC_SEQ_CST);
        return (prev & (1 << bit)) != 0 ? 1 : 0;
    }

    static char Y_FORCE_INLINE
    AtomicClearBit(volatile int& item, unsigned bit) {
        int prev = __atomic_fetch_and(&item, ~(1 << bit), __ATOMIC_SEQ_CST);
        return (prev & (1 << bit)) != 0 ? 1 : 0;
    }
#endif

#if defined(_x86_64_) || defined(_i386_) || defined (__aarch64__) || defined (__arm__) || defined (__powerpc64__)
    static bool AtomicLockHighByte(volatile int& item) {
        union TA {
            int x;
            char y[4];
        };

        volatile TA* ptr = reinterpret_cast<volatile TA*>(&item);
        char zero = 0;
        return __atomic_compare_exchange_n(&(ptr->y[3]), &zero, (char)128, true,
                                           __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);
    }

#endif

    template <typename TInt>
    static void Y_FORCE_INLINE AtomicStore(volatile TInt& var, TInt value) {
        __atomic_store_n(&var, value, __ATOMIC_RELEASE);
    }

    template <typename TInt>
    static void Y_FORCE_INLINE SequenceStore(volatile TInt& var, TInt value) {
        __atomic_store_n(&var, value, __ATOMIC_SEQ_CST);
    }

    template <typename TInt>
    static TInt Y_FORCE_INLINE AtomicLoad(const volatile TInt& var) {
        return __atomic_load_n(&var, __ATOMIC_ACQUIRE);
    }

    static void Y_FORCE_INLINE FutexWait(volatile int& fvar, int value) {
        for (;;) {
            int result =
                syscall(SYS_futex, &fvar, FUTEX_WAIT_PRIVATE, value, NULL, NULL, 0);
            if (Y_UNLIKELY(result == -1)) {
                if (errno == EWOULDBLOCK)
                    return;
                if (errno == EINTR)
                    continue;
                Y_ABORT("futex error");
            }
        }
    }

    static void Y_FORCE_INLINE FutexWake(volatile int& fvar, int amount) {
        const int result =
            syscall(SYS_futex, &fvar, FUTEX_WAKE_PRIVATE, amount, NULL, NULL, 0);
        if (Y_UNLIKELY(result == -1))
            Y_ABORT("futex error");
    }

}

class alignas(64) TLightRWLock {
public:
    TLightRWLock(ui32 spinCount = 10)
        : Counter_(0)
        , TrappedFutex_(0)
        , UnshareFutex_(0)
        , SpinCount_(spinCount)
    {
    }

    TLightRWLock(const TLightRWLock&) = delete;
    void operator=(const TLightRWLock&) = delete;

    Y_FORCE_INLINE void AcquireWrite() {
        using namespace NS_LightRWLock;

        if (AtomicLockHighByte(Counter_)) {
            if ((AtomicLoad(Counter_) & 0x7FFFFFFF) == 0)
                return;
            return WaitForUntrappedShared();
        }
        WaitForExclusiveAndUntrappedShared();
    }

    Y_FORCE_INLINE void AcquireRead() {
        using namespace NS_LightRWLock;

        if (Y_LIKELY(AtomicFetchAdd(Counter_, 1) >= 0))
            return;
        WaitForUntrappedAndAcquireRead();
    }

    Y_FORCE_INLINE void ReleaseWrite() {
        using namespace NS_LightRWLock;

        AtomicClearBit(Counter_, 31);
        if (AtomicLoad(TrappedFutex_)) {
            SequenceStore(TrappedFutex_, 0);
            FutexWake(TrappedFutex_, 0x7fffffff);
        }
    }

    Y_FORCE_INLINE void ReleaseRead() {
        using namespace NS_LightRWLock;

        if (Y_LIKELY(AtomicFetchAdd(Counter_, -1) >= 0))
            return;
        if (!AtomicLoad(UnshareFutex_))
            return;
        if ((AtomicLoad(Counter_) & 0x7fffffff) == 0) {
            SequenceStore(UnshareFutex_, 0);
            FutexWake(UnshareFutex_, 1);
        }
    }

private:
    volatile int Counter_;
    volatile int TrappedFutex_;
    volatile int UnshareFutex_;
    const ui32 SpinCount_;

    void WaitForUntrappedShared();
    void WaitForExclusiveAndUntrappedShared();
    void WaitForUntrappedAndAcquireRead();
};

#else

class TLightRWLock: public TRWMutex {
public:
    TLightRWLock() {
    }
    TLightRWLock(ui32) {
    }
};

#endif

using TLightReadGuard = TReadGuardBase<TLightRWLock>;
using TLightWriteGuard = TWriteGuardBase<TLightRWLock>;
