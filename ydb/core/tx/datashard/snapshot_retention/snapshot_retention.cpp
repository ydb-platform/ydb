#include "snapshot_retention.h"

#include <ydb/core/protos/long_tx_service_config.pb.h>

#include <algorithm>

namespace NKikimr::NDataShard {

TSnapshotRetentionPolicy::TSnapshotRetentionPolicy(bool enabled, const IImmutableSnapshotRegistryHolderPtr& holder,
    const NKikimrConfig::TLongTxServiceConfig& config)
    : Enabled_(enabled)
{
    if (!Enabled_) {
        return;
    }

    if (holder) {
        Registry_ = holder->Get();
    }
    if (!Registry_) {
        // Keep existing history until the registry has materialized, including
        // retained points below an already persisted low watermark.
        return;
    }

    // Each producer may use a different promotion delay. Its actual collection
    // cutoff protects unpublished snapshots even after a configuration change
    // or while exchange is stalled. Only clock skew remains local to cleanup.
    const ui64 cutoffStep = Registry_->GetMinPromotionCutoffStep();
    const ui64 clockSkewMs = config.GetMaxClockSkewMs();
    const ui64 safeStep = cutoffStep > clockSkewMs ? cutoffStep - clockSkewMs : 0;
    CleanupBoundary_ = std::min(TRowVersion(safeStep, 0), Registry_->GetBorder());
}

bool TSnapshotRetentionPolicy::IsWaitingForRegistry() const {
    return Enabled_ && !Registry_;
}

TRowVersion TSnapshotRetentionPolicy::GetCleanupBoundary(const TRowVersion& proposed) const {
    return Enabled_ ? std::min(proposed, CleanupBoundary_) : proposed;
}

bool TSnapshotRetentionPolicy::ShouldRetain(const TTableId& tableId, const TRowVersion& version) const {
    return Enabled_ && (!Registry_ || version >= CleanupBoundary_ || Registry_->HasSnapshot(tableId, version));
}

void TSnapshotRetentionPolicy::ForEachRetainedSnapshot(const TTableId& tableId,
    const TRowVersion& from, const TRowVersion& to,
    TFunctionRef<void(const TRowVersion&)> visitor) const
{
    if (Registry_) {
        Registry_->ForEachActiveSnapshot(tableId, from, to, visitor);
    }
}

} // namespace NKikimr::NDataShard
