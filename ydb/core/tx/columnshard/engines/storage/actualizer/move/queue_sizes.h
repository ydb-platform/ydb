#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <optional>

namespace NKikimr::NOlap::NActualizer {

// The gate needs only the total; the split says where a stalled move is stuck.
struct TMoveDataQueueSizes {
    ui64 Pending = 0;
    ui64 ConfirmedToMove = 0;
    ui64 InFlight = 0;
    // Uncommitted writes with blobs in the target groups: they cannot be rewritten, so the move waits for commit or abort.
    ui64 Uncommitted = 0;
    // Not a queue: deliberately outside GetTotal(), it explains where a drained queue went.
    ui64 Rejected = 0;

    ui64 GetTotal() const {
        return Pending + ConfirmedToMove + InFlight + Uncommitted;
    }

    TMoveDataQueueSizes& operator+=(const TMoveDataQueueSizes& item) {
        Pending += item.Pending;
        ConfirmedToMove += item.ConfirmedToMove;
        InFlight += item.InFlight;
        Uncommitted += item.Uncommitted;
        Rejected += item.Rejected;
        return *this;
    }
};

// FreezeCleanupWatermark raises the boundary to Max(maxPending, runningOldest) so that cleanup already in flight (portions already moved out of CleanupPortions) still blocks the gate.
inline TInstant FreezeCleanupWatermark(const TInstant maxPending, const std::optional<TInstant>& runningOldest) {
    return runningOldest ? Max(maxPending, *runningOldest) : maxPending;
}

}   // namespace NKikimr::NOlap::NActualizer
