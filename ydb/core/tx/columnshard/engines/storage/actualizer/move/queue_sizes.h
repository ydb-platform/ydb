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
    // Old portions rewritten by this session are in CleanupPortions awaiting DeclareRemove.
    BlockedByCleanup,
    BlockedByGC,
};

// Only portions at-or-before the watermark were produced by this move session.
inline bool CleanupBlocksGate(const std::optional<TInstant>& earliestCleanupInstant, const TInstant& watermark) {
    return earliestCleanupInstant.has_value() && *earliestCleanupInstant <= watermark;
}

inline EMoveDataGate ClassifyMoveDataGate(
    const bool vacuumCompleted, const TMoveDataQueueSizes& queues, const bool hasCleanupPortions, const bool hasBlobsForGroups) {
    if (!vacuumCompleted) {
        return EMoveDataGate::BlockedByVacuum;
    }
    if (queues.GetTotal() != 0) {
        return EMoveDataGate::BlockedByPortions;
    }
    if (hasCleanupPortions) {
        return EMoveDataGate::BlockedByCleanup;
    }
    if (hasBlobsForGroups) {
        return EMoveDataGate::BlockedByGC;
    }
    return EMoveDataGate::Ready;
}

}   // namespace NKikimr::NOlap::NActualizer
