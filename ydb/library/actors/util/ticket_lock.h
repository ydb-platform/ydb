#pragma once

#include "intrinsics.h"
#include <util/system/guard.h>
#include <util/system/yassert.h>

class TTicketLock : TNonCopyable {
    ui32 TicketIn;
    ui32 TicketOut;

public:
    TTicketLock()
        : TicketIn(0)
        , TicketOut(0)
    {
    }

    void Release() noexcept {
        AtomicUi32Increment(&TicketOut);
    }

    ui32 Acquire() noexcept {
        ui32 revolves = 0;
        const ui32 ticket = AtomicUi32Increment(&TicketIn) - 1;
        while (ticket != AtomicLoad(&TicketOut)) {
            Y_DEBUG_ABORT_UNLESS(ticket >= AtomicLoad(&TicketOut));
            SpinLockPause();
            ++revolves;
        }
        return revolves;
    }

    bool TryAcquire() noexcept {
        const ui32 x = AtomicLoad(&TicketOut);
        if (x == AtomicLoad(&TicketIn) && AtomicUi32Cas(&TicketIn, x + 1, x))
            return true;
        else
            return false;
    }

    bool IsLocked() noexcept {
        const ui32 ticketIn = AtomicLoad(&TicketIn);
        const ui32 ticketOut = AtomicLoad(&TicketOut);
        return (ticketIn != ticketOut);
    }

    typedef ::TGuard<TTicketLock> TGuard;
};
