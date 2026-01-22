#pragma once

#include <util/system/spinlock.h>
#include <util/system/guard.h>
#include <util/system/yassert.h>

class TTicketLock : TNonCopyable {
    std::atomic<ui32> TicketIn;
    std::atomic<ui32> TicketOut;

public:
    TTicketLock()
        : TicketIn(0)
        , TicketOut(0)
    {
    }

    void Release() noexcept {
        TicketOut.fetch_add(1, std::memory_order_release);
    }

    ui32 Acquire() noexcept {
        ui32 revolves = 0;
        const ui32 ticket = TicketIn.fetch_add(1, std::memory_order_seq_cst);
        while (ticket != TicketOut.load(std::memory_order_acquire)) {
            Y_DEBUG_ABORT_UNLESS(ticket >= TicketOut.load(std::memory_order_acquire));
            SpinLockPause();
            ++revolves;
        }
        return revolves;
    }

    bool TryAcquire() noexcept {
        ui32 x = TicketOut.load(std::memory_order_acquire);
        if (x == TicketIn.load(std::memory_order_acquire) &&
            TicketIn.compare_exchange_strong(x, x + 1, std::memory_order_acq_rel, std::memory_order_acquire))
            return true;
        else
            return false;
    }

    bool IsLocked() noexcept {
        const ui32 ticketIn = TicketIn.load(std::memory_order_acquire);
        const ui32 ticketOut = TicketOut.load(std::memory_order_acquire);
        return ticketIn != ticketOut;
    }

    using TGuard = ::TGuard<TTicketLock>;
};