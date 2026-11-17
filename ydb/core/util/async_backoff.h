#pragma once

#include "backoff.h"

#include <ydb/library/actors/async/event.h>
#include <ydb/library/actors/core/monotonic.h>

namespace NKikimr {

// One dependency's shared standard TBackoff, accessed only from its owning actor.
// The owner must outlive all Wait() coroutines. This is volatile state;
// callers remain responsible for durable recovery and RPC timeouts.
// Delays have millisecond precision; initialDelay must be at least 1 ms.
class TAsyncBackoff {
public:
    enum class EUpdate {
        Applied,
        Stale,
    };

    explicit TAsyncBackoff(TDuration initialDelay = TBackoff::DefaultInitialDelay,
                           TDuration maxDelay = TBackoff::DefaultMaxDelay);

    // Healthy calls proceed concurrently. During recovery, advance the version
    // before admitting one call; allow another after requestTimeout without an
    // outcome. Apply this same finite, positive timeout to the actual request.
    NActors::async<ui64> Wait(TDuration requestTimeout);
    EUpdate OnFailure(ui64 version);
    EUpdate OnSuccess(ui64 version);

    ui64 GetVersion() const;
    NActors::TMonotonic GetNextAllowedAt() const;

private:
    TBackoff Backoff;
    ui64 Version = 0;
    NActors::TMonotonic NextAllowedAt = NActors::TMonotonic::Zero();
    NActors::TAsyncEvent Changed;
    bool WakeupActive = false;
};

} // namespace NKikimr
