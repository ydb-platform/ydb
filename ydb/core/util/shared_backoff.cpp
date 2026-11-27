#include "shared_backoff.h"

namespace NKikimr {

TSharedBackoff::TSharedBackoff(TDuration initialDelay, TDuration maxDelay)
    : Backoff(initialDelay, maxDelay)
{
    Y_ABORT_UNLESS(initialDelay.MilliSeconds() > 0);
}

std::optional<ui64> TSharedBackoff::TryAcquire(NActors::TMonotonic now, TDuration reservation) {
    Y_ABORT_UNLESS(reservation > TDuration::Zero() && reservation != TDuration::Max());
    if (now < NextAllowedAt) {
        return std::nullopt;
    }
    if (Backoff.GetIteration() != 0) {
        // Fence earlier outcomes and other waiters before dispatching one probe.
        ++Version;
        NextAllowedAt = now + reservation;
    }
    return Version;
}

TSharedBackoff::EUpdate TSharedBackoff::OnFailure(ui64 version, NActors::TMonotonic now) {
    if (version != Version) {
        return EUpdate::Stale;
    }
    NextAllowedAt = now + Backoff.Next();
    ++Version;
    return EUpdate::Applied;
}

TSharedBackoff::EUpdate TSharedBackoff::OnSuccess(ui64 version) {
    if (version != Version) {
        return EUpdate::Stale;
    }
    if (Backoff.GetIteration() != 0) {
        Backoff.Reset();
        NextAllowedAt = NActors::TMonotonic::Zero();
        ++Version;
    }
    return EUpdate::Applied;
}

} // namespace NKikimr
