#include "lightrwlock.h"
#include <util/system/spinlock.h>

#if defined(_linux_) && !defined(_tsan_enabled_)

using namespace NS_LightRWLock;

void TLightRWLock::WaitForUntrappedShared() {
    for (;;) {
        for (ui32 i = 0; i < SpinCount_; ++i) {
            SpinLockPause();

            if ((AtomicLoad(Counter_) & 0x7FFFFFFF) == 0)
                return;
        }

        SequenceStore(UnshareFutex_, 1);
        if ((AtomicLoad(Counter_) & 0x7FFFFFFF) == 0) {
            AtomicStore(UnshareFutex_, 0);
            return;
        }
        FutexWait(UnshareFutex_, 1);
    }
}

void TLightRWLock::WaitForExclusiveAndUntrappedShared() {
    for (;;) {
        for (ui32 i = 0; i < SpinCount_; ++i) {
            SpinLockPause();

            if (AtomicLoad(Counter_) >= 0)
                goto try_to_get_lock;
            if (AtomicLoad(TrappedFutex_) == 1)
                goto skip_store_trapped;
        }

        SequenceStore(TrappedFutex_, 1);
    skip_store_trapped:

        if (AtomicLoad(Counter_) < 0) {
            FutexWait(TrappedFutex_, 1);
        }

    try_to_get_lock:
        if (!AtomicSetBit(Counter_, 31))
            break;
    }

    for (;;) {
        for (ui32 i = 0; i < SpinCount_; ++i) {
            if ((AtomicLoad(Counter_) & 0x7FFFFFFF) == 0)
                return;

            SpinLockPause();
        }

        SequenceStore(UnshareFutex_, 1);

        if ((AtomicLoad(Counter_) & 0x7FFFFFFF) == 0) {
            AtomicStore(UnshareFutex_, 0);
            return;
        }

        FutexWait(UnshareFutex_, 1);
    }
}

void TLightRWLock::WaitForUntrappedAndAcquireRead() {
    if (AtomicFetchAdd(Counter_, -1) < 0)
        goto skip_lock_try;

    for (;;) {
    again:
        if (Y_UNLIKELY(AtomicFetchAdd(Counter_, 1) >= 0)) {
            return;
        } else {
            if (AtomicFetchAdd(Counter_, -1) >= 0)
                goto again;
        }

    skip_lock_try:
        if (AtomicLoad(UnshareFutex_) && (AtomicLoad(Counter_) & 0x7FFFFFFF) == 0) {
            SequenceStore(UnshareFutex_, 0);
            FutexWake(UnshareFutex_, 1);
        }

        for (;;) {
            for (ui32 i = 0; i < SpinCount_; ++i) {
                SpinLockPause();

                if (AtomicLoad(Counter_) >= 0)
                    goto again;
                if (AtomicLoad(TrappedFutex_) == 1)
                    goto skip_store_trapped;
            }

            SequenceStore(TrappedFutex_, 1);
        skip_store_trapped:

            if (AtomicLoad(Counter_) < 0) {
                FutexWait(TrappedFutex_, 1);
                if (AtomicLoad(Counter_) < 0)
                    goto again;
            } else if (AtomicLoad(TrappedFutex_)) {
                SequenceStore(TrappedFutex_, 0);
                FutexWake(TrappedFutex_, 0x7fffffff);
            }
            break;
        }
    }
}

#endif // _linux_
