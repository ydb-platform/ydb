#include "async_backoff.h"

#include <ydb/library/actors/async/timeout.h>
#include <util/generic/scope.h>

namespace NKikimr {

TAsyncBackoff::TAsyncBackoff(TDuration initialDelay, TDuration maxDelay)
    : Backoff(initialDelay, maxDelay)
{
}

NActors::async<ui64> TAsyncBackoff::Wait(TDuration requestTimeout) {
    Y_ABORT_UNLESS(requestTimeout > TDuration::Zero() && requestTimeout != TDuration::Max());
    for (;;) {
        const auto now = NActors::TActivationContext::Monotonic();
        if (auto version = Backoff.TryAcquire(now, requestTimeout)) {
            co_return *version;
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
                    // Only ownership changed. Wake one successor; broadcasting
                    // here makes clustered caller cancellations quadratic.
                    Changed.NotifyOne();
                }
            };
            co_await NActors::WithDeadline(Backoff.GetNextAllowedAt(), [this]() -> NActors::async<void> {
                co_await Changed.Wait();
            });
        }
        // Outcomes, timeouts and cancellation of another waiter can all wake
        // us. Always recheck shared state after suspension.
    }
}

TAsyncBackoff::EUpdate TAsyncBackoff::OnFailure(ui64 version) {
    if (version != Backoff.GetVersion()) {
        return EUpdate::Stale;
    }
    Backoff.OnFailure(version, NActors::TActivationContext::Monotonic());
    Changed.NotifyAll();
    return EUpdate::Applied;
}

TAsyncBackoff::EUpdate TAsyncBackoff::OnSuccess(ui64 version) {
    if (version != Backoff.GetVersion()) {
        return EUpdate::Stale;
    }
    const auto previousVersion = Backoff.GetVersion();
    Backoff.OnSuccess(version);
    if (Backoff.GetVersion() != previousVersion) {
        Changed.NotifyAll();
    }
    return EUpdate::Applied;
}

ui64 TAsyncBackoff::GetVersion() const {
    return Backoff.GetVersion();
}

NActors::TMonotonic TAsyncBackoff::GetNextAllowedAt() const {
    return Backoff.GetNextAllowedAt();
}

} // namespace NKikimr
