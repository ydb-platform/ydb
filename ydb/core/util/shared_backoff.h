#pragma once

#include "backoff.h"
#include <ydb/library/actors/core/monotonic.h>
#include <optional>

namespace NKikimr {

// Volatile dependency health owned by one actor. The owner schedules wakeups
// and calls TryAcquire again after every wakeup; stale timers grant no permit.
class TSharedBackoff {
public:
    enum class EUpdate { Applied, Stale };

    explicit TSharedBackoff(TDuration initialDelay = TBackoff::DefaultInitialDelay,
                            TDuration maxDelay = TBackoff::DefaultMaxDelay);
    std::optional<ui64> TryAcquire(NActors::TMonotonic now, TDuration reservation);
    EUpdate OnFailure(ui64 version, NActors::TMonotonic now);
    EUpdate OnSuccess(ui64 version);
    ui64 GetVersion() const { return Version; }
    NActors::TMonotonic GetNextAllowedAt() const { return NextAllowedAt; }

private:
    TBackoff Backoff;
    ui64 Version = 0;
    NActors::TMonotonic NextAllowedAt = NActors::TMonotonic::Zero();
};

} // namespace NKikimr
