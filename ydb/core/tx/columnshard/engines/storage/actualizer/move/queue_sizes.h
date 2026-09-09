#pragma once

#include <util/system/types.h>

namespace NKikimr::NOlap::NActualizer {

// The gate needs only the total; the split says where a stalled move is stuck.
struct TMoveDataQueueSizes {
    ui64 Pending = 0;
    ui64 ConfirmedToMove = 0;
    ui64 InFlight = 0;
    // Not a queue: deliberately outside GetTotal(), it explains where a drained queue went.
    ui64 Rejected = 0;

    ui64 GetTotal() const {
        return Pending + ConfirmedToMove + InFlight;
    }

    TMoveDataQueueSizes& operator+=(const TMoveDataQueueSizes& item) {
        Pending += item.Pending;
        ConfirmedToMove += item.ConfirmedToMove;
        InFlight += item.InFlight;
        Rejected += item.Rejected;
        return *this;
    }
};

enum class EMoveDataGate {
    Ready,
    BlockedByVacuum,
    BlockedByPortions,
    BlockedByGC,
};

inline EMoveDataGate ClassifyMoveDataGate(const bool vacuumCompleted, const TMoveDataQueueSizes& queues, const bool hasBlobsForGroups) {
    if (!vacuumCompleted) {
        return EMoveDataGate::BlockedByVacuum;
    }
    if (queues.GetTotal() != 0) {
        return EMoveDataGate::BlockedByPortions;
    }
    if (hasBlobsForGroups) {
        return EMoveDataGate::BlockedByGC;
    }
    return EMoveDataGate::Ready;
}

}   // namespace NKikimr::NOlap::NActualizer
