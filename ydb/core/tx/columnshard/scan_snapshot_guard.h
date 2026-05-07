#pragma once

#include "common/path_id.h"
#include "common/snapshot.h"
#include "engines/snapshot_holders.h"
#include "inflight_request_tracker.h"

#include <memory>

namespace NKikimr::NColumnShard {

class IScanSnapshotGuard {
public:
    virtual ~IScanSnapshotGuard() = default;
    virtual NOlap::TSnapshot GetMinSnapshotForNewReads() const = 0;
    virtual bool MayStartScanAt(const NOlap::TSnapshot& snapshot, const TSchemeShardLocalPathId& schemeShardLocalPathId) const = 0;
    virtual std::unique_ptr<NOlap::ISnapshotHolders> BuildSnapshotHolders() const = 0;
};

std::unique_ptr<IScanSnapshotGuard> CreateScanSnapshotGuard(
    ui64 passedStep,
    ui64 schemeShardId,
    const TInFlightReadsTracker& inFlightReadsTracker,
    const NOlap::IPathIdTranslator& pathIdTranslator);

}   // namespace NKikimr::NColumnShard

