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

    // A snapshot may be absent from a collection while it is too young to be
    // published. Anchor cleanup to collection time, so a stalled exchange cannot
    // make an unseen snapshot eligible for removal as wall-clock time advances.
    const TDuration margin = TDuration::Seconds(config.GetLocalSnapshotPromotionTimeSeconds())
        + TDuration::MilliSeconds(config.GetMaxClockSkewMs());
    const TInstant freshnessFloor = Registry_->GetOldestCollectionTime() - margin;
    CleanupBoundary_ = std::min(TRowVersion(freshnessFloor.MilliSeconds(), 0), Registry_->GetBorder());
}

TRowVersion TSnapshotRetentionPolicy::GetCleanupBoundary(const TRowVersion& proposed) const {
    return Enabled_ ? std::min(proposed, CleanupBoundary_) : proposed;
}

bool TSnapshotRetentionPolicy::ShouldRetain(const TTableId& tableId, const TRowVersion& version) const {
    return Enabled_ && (!Registry_ || version >= CleanupBoundary_ || Registry_->HasSnapshot(tableId, version));
}

TSet<TRowVersion> TSnapshotRetentionPolicy::GetRetainedSnapshots(const TTableId& tableId) const {
    return Registry_ ? Registry_->GetActiveSnapshots(tableId) : TSet<TRowVersion>{};
}

} // namespace NKikimr::NDataShard
