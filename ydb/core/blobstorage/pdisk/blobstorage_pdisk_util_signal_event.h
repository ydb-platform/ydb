#pragma once

#include "defs.h"

#include <util/generic/ptr.h>
#include <util/datetime/base.h>

class TSignalEvent {
public:
    TSignalEvent();
    TSignalEvent(const TSignalEvent& other) noexcept;
    TSignalEvent& operator=(const TSignalEvent& other) noexcept;

    ~TSignalEvent();

    void Reset() noexcept;
    void Signal() noexcept;

    /*
     * return true if signaled, false if timed out.
     */
    bool WaitD(TInstant deadLine) noexcept;

    inline bool WaitT(TDuration timeOut) noexcept {
        return WaitD(timeOut.ToDeadLine());
    }

    /*
     * wait infinite time
     */
    inline void WaitI() noexcept {
        WaitD(TInstant::Max());
    }

private:
    class TEvImpl;
    TIntrusivePtr<TEvImpl> EvImpl_;
};
