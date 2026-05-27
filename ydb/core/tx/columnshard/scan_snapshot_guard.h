#pragma once

#include "inflight_request_tracker.h"

#include "common/path_id.h"
#include "common/snapshot.h"
#include "engines/snapshot_holders.h"

#include <memory>

namespace NKikimrConfig {
class TLongTxServiceConfig;
}

namespace NKikimr::NColumnShard {

class IScanSnapshotGuard {
public:
    virtual ~IScanSnapshotGuard() = default;
    virtual NOlap::TSnapshot GetMinSnapshotForNewReads() const = 0;
    virtual bool MayStartScanAt(const NOlap::TSnapshot& snapshot, const TSchemeShardLocalPathId& schemeShardLocalPathId) const = 0;
    virtual std::unique_ptr<NOlap::ISnapshotHolders> BuildSnapshotHolders() const = 0;
};

std::unique_ptr<IScanSnapshotGuard> CreateScanSnapshotGuard(ui64 passedStep, ui64 schemeShardId, const NOlap::TSnapshot& lastCleanupSnapshot,
    const TInFlightReadsTracker& inFlightReadsTracker, const NOlap::IPathIdTranslator& pathIdTranslator);

std::unique_ptr<IScanSnapshotGuard> CreateLocalScanSnapshotGuard(
    ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot, const TInFlightReadsTracker& inFlightReadsTracker);

std::unique_ptr<IScanSnapshotGuard> CreateRegistryNotReadySnapshotGuard();

std::unique_ptr<IScanSnapshotGuard> CreateRegistryScanSnapshotGuard(ui64 passedStep, ui64 schemeShardId,
    const NOlap::TSnapshot& lastCleanupSnapshot, const NOlap::IPathIdTranslator& pathIdTranslator,
    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> registry, const NKikimrConfig::TLongTxServiceConfig& longTxConfig);

}   // namespace NKikimr::NColumnShard
