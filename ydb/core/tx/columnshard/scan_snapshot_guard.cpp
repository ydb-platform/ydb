#include "scan_snapshot_guard.h"

#include "hooks/abstract/abstract.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

namespace NKikimr::NColumnShard {
namespace {

NOlap::TSnapshot BuildLocalMinSnapshotForNewReads(const ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot) {
    const ui64 delayMillisec = NYDBTest::TControllers::GetColumnShardController()->GetMaxReadStaleness().MilliSeconds();
    const ui64 minReadStep = (passedStep > delayMillisec ? passedStep - delayMillisec : 0);
    return std::max(NOlap::TSnapshot(minReadStep, 0), lastCleanupSnapshot);
}

class TLocalScanSnapshotGuard: public IScanSnapshotGuard {
private:
    const TInFlightReadsTracker& InFlightReadsTracker;
    const NOlap::TSnapshot MinSnapshotForNewReads;

public:
    TLocalScanSnapshotGuard(
        const ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot, const TInFlightReadsTracker& inFlightReadsTracker)
        : InFlightReadsTracker(inFlightReadsTracker)
        , MinSnapshotForNewReads(BuildLocalMinSnapshotForNewReads(passedStep, lastCleanupSnapshot))
    {
    }

    NOlap::TSnapshot GetMinSnapshotForNewReads() const override {
        return MinSnapshotForNewReads;
    }

    bool MayStartScanAt(const NOlap::TSnapshot& snapshot, const TSchemeShardLocalPathId&) const override {
        return MinSnapshotForNewReads <= snapshot || InFlightReadsTracker.HasLiveSnapshot(snapshot);
    }

    std::unique_ptr<NOlap::ISnapshotHolders> BuildSnapshotHolders() const override {
        auto inFlightTxs = InFlightReadsTracker.GetLiveSnapshots(MinSnapshotForNewReads);
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
    // How long it may take for a snapshot from the most remote node to reach this columnshard
    // in the worst case by default:
    //   promotion delay (120s) + exchange period (10s) + registry rebuild period (30s) + network/propagation delta (10s) = 170s.
    // 10 seconds delta is the agreed conservative estimate:
    //   4 messages * log_10(100k nodes) * 0.5s (ping time between the nodes) = 10s.

    // But, in fact, the registry is not ready right after the node start, it takes it a while to materialize.
    // During this period of materialization the shard keeps all portions (do not delete anything), see `TRegistryNotReadySnapshotGuard`.
    // A practical upper estimate of this additional startup "no-delete" window with default config:
    // exchange period (10s) + registry rebuild period (30s) + network/propagation delta (10s) = 50s
    // Therefore the total default conservative duration of keeping removed portions is roughly:
    //   170s (steady-state delay window) + 50s (startup materialization period) = ~220 seconds.
    // That is at most for how long we will keep removed portions by default.
    const ui64 delaySeconds = longTxConfig.GetLocalSnapshotPromotionTimeSeconds() + longTxConfig.GetSnapshotsExchangeIntervalSeconds() +
                              longTxConfig.GetSnapshotsRegistryUpdateIntervalSeconds() + 10;
    const ui64 delayMillisec = TDuration::Seconds(delaySeconds).MilliSeconds();
    const ui64 serviceMinReadStep = (passedStep > delayMillisec ? passedStep - delayMillisec : 0);

    NOlap::TSnapshot minSnapshot = std::max(NOlap::TSnapshot(serviceMinReadStep, 0), lastCleanupSnapshot);
    // Registry border may be older than the calculated delay window.
    // In that case we use the older border to preserve correctness.
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
        return CreateLocalScanSnapshotGuard(passedStep, lastCleanupSnapshot, inFlightReadsTracker);
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

std::unique_ptr<IScanSnapshotGuard> CreateLocalScanSnapshotGuard(
    ui64 passedStep, const NOlap::TSnapshot& lastCleanupSnapshot, const TInFlightReadsTracker& inFlightReadsTracker) {
    return std::make_unique<TLocalScanSnapshotGuard>(passedStep, lastCleanupSnapshot, inFlightReadsTracker);
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
