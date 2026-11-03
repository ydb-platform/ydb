#include "async_backoff.h"

#include <ydb/library/actors/async/timeout.h>
#include <util/generic/scope.h>

namespace NKikimr {

TAsyncBackoff::TAsyncBackoff(TDuration initialDelay, TDuration maxDelay)
    : Backoff(initialDelay, maxDelay)
{
    Y_ABORT_UNLESS(initialDelay.MilliSeconds() > 0);
}

NActors::async<ui64> TAsyncBackoff::Wait(TDuration requestTimeout) {
    Y_ABORT_UNLESS(requestTimeout > TDuration::Zero() && requestTimeout != TDuration::Max());
    for (;;) {
        const auto now = NActors::TActivationContext::Monotonic();
        if (now >= NextAllowedAt) {
            if (Backoff.GetIteration() != 0) {
                // Fence both previous outcomes and other waiters before dispatch.
                ++Version;
                NextAllowedAt = now + requestTimeout;
            }
            co_return Version;
        }

        if (WakeupActive) {
            co_await Changed.Wait();
        } else {
            // One waiting coroutine owns the active timer. Cancelling it hands
            // that responsibility to a remaining waiter; no detached task lives
            // beyond the callers' lifetimes.
            WakeupActive = true;
            Y_DEFER {
                WakeupActive = false;
                // Hard mailbox teardown destroys frames without actor TLS.
                if (NActors::TlsActivationContext) {
                    Changed.NotifyAll();
                }
            };
            co_await NActors::WithDeadline(NextAllowedAt, [this]() -> NActors::async<void> {
                co_await Changed.Wait();
            });
        }
        // Outcomes, timeouts and cancellation of another waiter can all wake
        // us. Always recheck shared state after suspension.
    }
}

TAsyncBackoff::EUpdate TAsyncBackoff::OnFailure(ui64 version) {
    if (version != Version) {
        return EUpdate::Stale;
    }
    NextAllowedAt = NActors::TActivationContext::Monotonic() + Backoff.Next();
    ++Version;
    Changed.NotifyAll();
    return EUpdate::Applied;
}

TAsyncBackoff::EUpdate TAsyncBackoff::OnSuccess(ui64 version) {
    if (version != Version) {
        return EUpdate::Stale;
    }
    if (Backoff.GetIteration() != 0) {
        Backoff.Reset();
        NextAllowedAt = NActors::TMonotonic::Zero();
        ++Version;
        Changed.NotifyAll();
    }
    return EUpdate::Applied;
}

ui64 TAsyncBackoff::GetVersion() const {
    return Version;
}

NActors::TMonotonic TAsyncBackoff::GetNextAllowedAt() const {
    return NextAllowedAt;
}

} // namespace NKikimr
