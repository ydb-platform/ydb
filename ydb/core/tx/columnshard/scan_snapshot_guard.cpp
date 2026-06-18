#include "scan_snapshot_guard.h"

#include "hooks/abstract/abstract.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <algorithm>

namespace NKikimr::NColumnShard {
namespace {

NOlap::TSnapshot BuildLocalMinSnapshotForNewReads(const ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot) {
    const ui64 delayMillisec = NYDBTest::TControllers::GetColumnShardController()->GetMaxReadStaleness().MilliSeconds();
    const ui64 minReadStep = (passedStep > delayMillisec ? passedStep - delayMillisec : 0);
    return std::max(NOlap::TSnapshot(minReadStep, 0), lastCleanupSnapshot);
}

std::vector<NOlap::TSnapshot> BuildReadOnlyTablesPinnedSnapshots(
    const NOlap::IPathIdTranslator& pathIdTranslator, const NOlap::TSnapshot minSnapshotForNewReads) {
    std::vector<NOlap::TSnapshot> readOnlySnapshots;
    for (const auto& readOnlySnapshot : pathIdTranslator.GetReadOnlyTablesSnapshots()) {
        if (readOnlySnapshot < minSnapshotForNewReads) {
            readOnlySnapshots.emplace_back(readOnlySnapshot);
        }
    }
    std::sort(readOnlySnapshots.begin(), readOnlySnapshots.end());
    return readOnlySnapshots;
}

std::vector<NOlap::TSnapshot> MergePinnedSnapshots(
    std::vector<NOlap::TSnapshot> inFlightTxs, const NOlap::IPathIdTranslator& pathIdTranslator, const NOlap::TSnapshot minSnapshotForNewReads) {
    auto readOnlySnapshots = BuildReadOnlyTablesPinnedSnapshots(pathIdTranslator, minSnapshotForNewReads);
    inFlightTxs.insert(inFlightTxs.end(), readOnlySnapshots.begin(), readOnlySnapshots.end());
    std::sort(inFlightTxs.begin(), inFlightTxs.end());
    inFlightTxs.erase(std::unique(inFlightTxs.begin(), inFlightTxs.end()), inFlightTxs.end());
    return inFlightTxs;
}

class TLocalScanSnapshotGuard: public IScanSnapshotGuard {
private:
    const TInFlightReadsTracker& InFlightReadsTracker;
    const NOlap::IPathIdTranslator& PathIdTranslator;
    const NOlap::TSnapshot MinSnapshotForNewReads;

public:
    TLocalScanSnapshotGuard(const ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot,
        const TInFlightReadsTracker& inFlightReadsTracker, const NOlap::IPathIdTranslator& pathIdTranslator)
        : InFlightReadsTracker(inFlightReadsTracker)
        , PathIdTranslator(pathIdTranslator)
        , MinSnapshotForNewReads(BuildLocalMinSnapshotForNewReads(passedStep, lastCleanupSnapshot))
    {
    }

    NOlap::TSnapshot GetMinSnapshotForNewReads() const override {
        return MinSnapshotForNewReads;
    }

    bool MayStartScanAt(const NOlap::TSnapshot& snapshot, const TSchemeShardLocalPathId& schemeShardLocalPathId) const override {
        if (PathIdTranslator.GetCopyVersionOptional(schemeShardLocalPathId)) {
            return true;
        }
        return MinSnapshotForNewReads <= snapshot || InFlightReadsTracker.HasLiveSnapshot(snapshot);
    }

    std::unique_ptr<NOlap::ISnapshotHolders> BuildSnapshotHolders() const override {
        auto inFlightTxs = InFlightReadsTracker.GetLiveSnapshots(MinSnapshotForNewReads);
        inFlightTxs = MergePinnedSnapshots(std::move(inFlightTxs), PathIdTranslator, MinSnapshotForNewReads);
        return std::make_unique<NOlap::TLegacySnapshotHolders>(MinSnapshotForNewReads, std::move(inFlightTxs));
    }
};

class TRegistryNotReadySnapshotGuard: public IScanSnapshotGuard {
private:
    const NOlap::TSnapshot MinSnapshotForNewReads = NOlap::TSnapshot::Zero();

public:
    NOlap::TSnapshot GetMinSnapshotForNewReads() const override {
        // Strictly safe mode while snapshots locking is enabled but registry is not materialized yet:
        // no portion is considered removable by snapshot age.
        return MinSnapshotForNewReads;
    }

    bool MayStartScanAt(const NOlap::TSnapshot&, const TSchemeShardLocalPathId&) const override {
        // During warm-up we accept scan starts and rely on conservative cleanup policy above.
        return true;
    }

    std::unique_ptr<NOlap::ISnapshotHolders> BuildSnapshotHolders() const override {
        // Keep everything until registry materializes to preserve correctness.
        return std::make_unique<NOlap::TLegacySnapshotHolders>(MinSnapshotForNewReads, std::vector<NOlap::TSnapshot>{});
    }
};

NOlap::TSnapshot BuildRegistryMinSnapshotForNewReads(const ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot,
    const TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>& registry, const NKikimrConfig::TLongTxServiceConfig& longTxConfig) {
    // OldestCollectionTime tells us how fresh the registry actually is (oldest collection time
    // across all contributing nodes). Anchoring on it means the floor backs off on its own when
    // the registry is stale (e.g. under CPU saturation), instead of trusting a fixed worst-case
    // estimate. A read on a remote node may still be missing from the registry for up to:
    //   - LocalSnapshotPromotionTimeSeconds: a snapshot isn't propagated until it is promoted;
    //   - MaxClockSkewMs: that collection time was stamped against the remote node's wall clock.
    // So we may only clean strictly below OldestCollectionTime minus that margin.
    const ui64 marginMs =
        TDuration::Seconds(longTxConfig.GetLocalSnapshotPromotionTimeSeconds()).MilliSeconds() + longTxConfig.GetMaxClockSkewMs();
    const ui64 oldestCollectionMs = registry->GetOldestCollectionTime().MilliSeconds();
    ui64 serviceMinReadStep = (oldestCollectionMs > marginMs ? oldestCollectionMs - marginMs : 0);
    // The freshness floor must never exceed the current step.
    serviceMinReadStep = std::min(serviceMinReadStep, passedStep);

    NOlap::TSnapshot minSnapshot = std::max(NOlap::TSnapshot(serviceMinReadStep, 0), lastCleanupSnapshot);
    // Registry border may be older than the computed floor; use the older to preserve correctness.
    const TRowVersion border = registry->GetBorder();
    return std::min(minSnapshot, NOlap::TSnapshot(border.Step, border.TxId));
}

class TRegistryScanSnapshotGuard: public IScanSnapshotGuard {
private:
    const ui64 SchemeShardId;
    const NOlap::IPathIdTranslator& PathIdTranslator;
    const TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> Registry;
    const NOlap::TSnapshot MinSnapshotForNewReads;

public:
    TRegistryScanSnapshotGuard(const ui64 passedStep, const ui64 schemeShardId, const NOlap::TSnapshot& lastCleanupSnapshot,
        const NOlap::IPathIdTranslator& pathIdTranslator, TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> registry,
        const NKikimrConfig::TLongTxServiceConfig& longTxConfig)
        : SchemeShardId(schemeShardId)
        , PathIdTranslator(pathIdTranslator)
        , Registry(std::move(registry))
        , MinSnapshotForNewReads(BuildRegistryMinSnapshotForNewReads(passedStep, lastCleanupSnapshot, Registry, longTxConfig))
    {
        AFL_VERIFY(Registry);
    }

    NOlap::TSnapshot GetMinSnapshotForNewReads() const override {
        return MinSnapshotForNewReads;
    }

    bool MayStartScanAt(const NOlap::TSnapshot& snapshot, const TSchemeShardLocalPathId& schemeShardLocalPathId) const override {
        if (const auto copySnapshot = PathIdTranslator.GetCopyVersionOptional(schemeShardLocalPathId)) {
            AFL_VERIFY(*copySnapshot == snapshot);
            return true;
        }
        if (MinSnapshotForNewReads <= snapshot) {
            return true;
        }

        // Registry has, in fact, all the schemaVersion = 0. So we use here 0 instead of real one.
        const NKikimr::TTableId tableId(SchemeShardId, schemeShardLocalPathId.GetRawValue(), 0);
        return Registry->HasSnapshot(tableId, TRowVersion(snapshot.GetPlanStep(), snapshot.GetTxId()));
    }

    std::unique_ptr<NOlap::ISnapshotHolders> BuildSnapshotHolders() const override {
        return std::make_unique<NOlap::TRegistrySnapshotHolders>(MinSnapshotForNewReads, Registry, SchemeShardId, PathIdTranslator);
    }
};

}   // namespace

std::unique_ptr<IScanSnapshotGuard> CreateScanSnapshotGuard(ui64 passedStep, ui64 schemeShardId, const NOlap::TSnapshot& lastCleanupSnapshot,
    const TInFlightReadsTracker& inFlightReadsTracker, const NOlap::IPathIdTranslator& pathIdTranslator) {
    if (!HasAppData() || !AppDataVerified().FeatureFlags.GetEnableSnapshotsLocking()) {
        return CreateLocalScanSnapshotGuard(passedStep, lastCleanupSnapshot, inFlightReadsTracker, pathIdTranslator);
    }

    if (const auto holder = AppDataVerified().SnapshotRegistryHolder) {
        if (const auto registry = holder->Get()) {
            return CreateRegistryScanSnapshotGuard(
                passedStep, schemeShardId, lastCleanupSnapshot, pathIdTranslator, registry, AppDataVerified().LongTxServiceConfig);
        }
    }

    // Snapshots locking is enabled, but registry isn't ready yet on this node.
    // Use explicit warm-up guard to keep behavior safe and easy to reason about.
    return CreateRegistryNotReadySnapshotGuard();
}

std::unique_ptr<IScanSnapshotGuard> CreateLocalScanSnapshotGuard(const ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot,
    const TInFlightReadsTracker& inFlightReadsTracker, const NOlap::IPathIdTranslator& pathIdTranslator) {
    return std::make_unique<TLocalScanSnapshotGuard>(passedStep, lastCleanupSnapshot, inFlightReadsTracker, pathIdTranslator);
}

std::unique_ptr<IScanSnapshotGuard> CreateRegistryNotReadySnapshotGuard() {
    return std::make_unique<TRegistryNotReadySnapshotGuard>();
}

std::unique_ptr<IScanSnapshotGuard> CreateRegistryScanSnapshotGuard(ui64 passedStep, ui64 schemeShardId,
    const NOlap::TSnapshot& lastCleanupSnapshot, const NOlap::IPathIdTranslator& pathIdTranslator,
    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> registry, const NKikimrConfig::TLongTxServiceConfig& longTxConfig) {
    return std::make_unique<TRegistryScanSnapshotGuard>(
        passedStep, schemeShardId, lastCleanupSnapshot, pathIdTranslator, std::move(registry), longTxConfig);
}

}   // namespace NKikimr::NColumnShard
