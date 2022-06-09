#include "blobstorage_pdisk_util_signal_event.h"

#include <util/system/datetime.h>
#include <util/system/defaults.h>

#include <cstdio>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>

class TSignalEvent::TEvImpl: public TThrRefBase {
public:
#ifdef _win_
    inline TEvImpl() {
        cond = CreateEvent(nullptr, false, false, nullptr);
    }

    inline ~TEvImpl() {
        CloseHandle(cond);
    }

    inline void Reset() noexcept {
        ResetEvent(cond);
    }

    inline void Signal() noexcept {
        SetEvent(cond);
    }

    inline bool WaitD(TInstant deadLine) noexcept {
        if (deadLine == TInstant::Max()) {
            return WaitForSingleObject(cond, INFINITE) == WAIT_OBJECT_0;
        }

        const TInstant now = Now();

        if (now < deadLine) {
            //TODO
            return WaitForSingleObject(cond, (deadLine - now).MilliSeconds()) == WAIT_OBJECT_0;
        }

        return (WaitForSingleObject(cond, 0) == WAIT_OBJECT_0);
    }

#else

    inline void Signal() noexcept {
        with_lock (Mutex) {
            AtomicSet(Signaled, 1);
            Cond.BroadCast();
        }
    }

    inline void Reset() noexcept {
        AtomicSet(Signaled, 0);
    }

    inline bool WaitD(TInstant deadLine) noexcept {
        bool resSignaled = true;

        with_lock (Mutex) {
            while (!AtomicGet(Signaled)) {
                if (!Cond.WaitD(Mutex, deadLine)) {
                    resSignaled = AtomicGet(Signaled); // timed out, but Signaled could have been set

                    break;
                }
            }

            AtomicSet(Signaled, 0);
        }

        return resSignaled;
    }
#endif

private:
#ifdef _win_
    HANDLE cond;
#else
    TCondVar Cond;
    TMutex Mutex;
    TAtomic Signaled = 0;
#endif
};

TSignalEvent::TSignalEvent()
    : EvImpl_(new TEvImpl())
{
}

TSignalEvent::TSignalEvent(const TSignalEvent& other) noexcept
    : EvImpl_(other.EvImpl_)
{
}

TSignalEvent& TSignalEvent::operator=(const TSignalEvent& other) noexcept {
    EvImpl_ = other.EvImpl_;
    return *this;
}

TSignalEvent::~TSignalEvent() = default;

void TSignalEvent::Reset() noexcept {
    EvImpl_->Reset();
}

void TSignalEvent::Signal() noexcept {
    EvImpl_->Signal();
}

bool TSignalEvent::WaitD(TInstant deadLine) noexcept {
    return EvImpl_->WaitD(deadLine);
}
