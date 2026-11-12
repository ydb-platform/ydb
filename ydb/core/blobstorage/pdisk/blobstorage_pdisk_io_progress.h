#pragma once

#include <ydb/library/actors/core/monotonic.h>
#include <util/system/types.h>

#include <optional>

namespace NKikimr::NPDisk {

// Actor-owned observation of the backend's cumulative admission/retirement counters.
// New admissions do not restart an existing period with no retirement. Observing
// a retirement starts a new period, so sampling conservatively understates its age.
// A counter or clock anomaly makes the current observation unavailable.
class TIoProgressTracker {
public:
    struct TSample {
        ui64 PendingOperations = 0;
        ui64 NoCompletionMs = 0;
    };

    void Reset() {
        BusySince.reset();
        LastSample.reset();
        LastTakeoffs = 0;
        LastLandings = 0;
    }

    std::optional<TSample> Update(NActors::TMonotonic now, i64 takeoffs, i64 landings) {
        if (takeoffs < 0 || landings < 0 || landings > takeoffs ||
                (LastSample && (now < *LastSample || takeoffs < LastTakeoffs || landings < LastLandings))) {
            Reset();
            return std::nullopt;
        }

        const ui64 pending = takeoffs - landings;
        if (!BusySince || landings != LastLandings || !pending) {
            BusySince = pending ? std::make_optional(now) : std::nullopt;
        }
        LastSample = now;
        LastTakeoffs = takeoffs;
        LastLandings = landings;
        return TSample{pending, BusySince ? (now - *BusySince).MilliSeconds() : 0};
    }

private:
    std::optional<NActors::TMonotonic> BusySince;
    std::optional<NActors::TMonotonic> LastSample;
    i64 LastTakeoffs = 0;
    i64 LastLandings = 0;
};

} // namespace NKikimr::NPDisk
