#pragma once

#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

namespace NKikimrConfig {
class TLongTxServiceConfig;
} // namespace NKikimrConfig

namespace NKikimr::NDataShard {

// Captures one immutable registry view for a cleanup or read operation.
// Local reads and persistent snapshots must be protected by the caller as well.
class TSnapshotRetentionPolicy {
public:
    TSnapshotRetentionPolicy(bool enabled, const IImmutableSnapshotRegistryHolderPtr& holder,
        const NKikimrConfig::TLongTxServiceConfig& config);

    // Distinguishes missing registry state from a ready registry with a zero boundary.
    bool IsWaitingForRegistry() const;

    // Limits an existing cleanup candidate. Cleanup may remove versions strictly
    // below this boundary, except for individually retained snapshots.
    TRowVersion GetCleanupBoundary(const TRowVersion& proposed) const;

    // Includes the freshness window and border. Returns true for every version
    // while the registry is not ready. Retention does not prove readability:
    // callers must still reject versions whose history has already been removed.
    bool ShouldRetain(const TTableId& tableId, const TRowVersion& version) const;

    // Visit explicit points in [from, to) in ascending order without duplicates
    // or copying the registry. Includes all-table registrations; the continuous
    // protected range is represented by GetCleanupBoundary().
    void ForEachRetainedSnapshot(const TTableId& tableId,
        const TRowVersion& from, const TRowVersion& to,
        TFunctionRef<void(const TRowVersion&)> visitor) const;

private:
    const bool Enabled_;
    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> Registry_;
    TRowVersion CleanupBoundary_ = TRowVersion::Min();
};

} // namespace NKikimr::NDataShard
