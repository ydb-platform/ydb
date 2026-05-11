#include "test_path_id_translator.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/scan_snapshot_guard.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard {

Y_UNIT_TEST_SUITE(TScanSnapshotGuardTests) {
    NOlap::TSnapshot Step(const ui64 planStep) {
        return NOlap::TSnapshot(planStep, 1);
    }

    TInFlightReadsTracker MakeTracker() {
        return TInFlightReadsTracker(nullptr, nullptr);
    }

    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> CreateSnapshotRegistry(
        const std::optional<TRowVersion>& border = std::nullopt, const std::vector<std::pair<NKikimr::TTableId, TRowVersion>>& snapshots = {}) {
        auto registryBuilder = CreateImmutableSnapshotRegistryBuilder();
        if (border) {
            registryBuilder->SetSnapshotBorder(*border);
        }
        for (const auto& [tableId, snapshot] : snapshots) {
            registryBuilder->AddSnapshot({ tableId }, snapshot);
        }
        return TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>(std::move(*registryBuilder).Build().release());
    }

    NKikimrConfig::TLongTxServiceConfig MakeExplicitLongTxConfig() {
        NKikimrConfig::TLongTxServiceConfig config;
        config.SetLocalSnapshotPromotionTimeSeconds(120);
        config.SetSnapshotsExchangeIntervalSeconds(10);
        config.SetSnapshotsRegistryUpdateIntervalSeconds(30);
        return config;
    }

    Y_UNIT_TEST(LocalGuardSmoke) {
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csControllerGuard->SetOverrideMaxReadStaleness(TDuration::MilliSeconds(250));

        auto tracker = MakeTracker();
        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();
        auto guard = CreateLocalScanSnapshotGuard(/*passedStep*/ 1000, lastCleanupSnapshot, tracker);

        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads().GetPlanStep(), 750);
        UNIT_ASSERT(guard->MayStartScanAt(Step(1000), TSchemeShardLocalPathId::FromRawValue(1)));
        UNIT_ASSERT(!guard->MayStartScanAt(Step(700), TSchemeShardLocalPathId::FromRawValue(1)));

        auto holders = guard->BuildSnapshotHolders();
        UNIT_ASSERT(holders);
        UNIT_ASSERT_VALUES_EQUAL(holders->GetMinSnapshotForNewReads().GetPlanStep(), 750);
    }

    Y_UNIT_TEST(RegistryNotReadyGuardSmoke) {
        auto guard = CreateRegistryNotReadySnapshotGuard();

        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads(), NOlap::TSnapshot::Zero());
        UNIT_ASSERT(guard->MayStartScanAt(Step(1), TSchemeShardLocalPathId::FromRawValue(1)));

        auto holders = guard->BuildSnapshotHolders();
        UNIT_ASSERT(holders);
        UNIT_ASSERT_VALUES_EQUAL(holders->GetMinSnapshotForNewReads(), NOlap::TSnapshot::Zero());
    }

    Y_UNIT_TEST(RegistryGuardWithBorder) {
        const auto longTxConfig = MakeExplicitLongTxConfig();
        const ui64 schemeShardId = 123;
        NOlap::NTest::TTestPathIdTranslator translator;
        const auto internalPathId = TInternalPathId::FromRawValue(7);
        const auto ssPathId = TSchemeShardLocalPathId::FromRawValue(70);
        translator.Add(internalPathId, { ssPathId });

        auto registry = CreateSnapshotRegistry(TRowVersion(900, 0));

        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, translator, registry, longTxConfig);

        // default delay = 120 + 10 + 30 + 10 = 170 seconds -> service min step = 30000
        // border = 900 -> effective min step = min(30000, 900) = 900
        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads().GetPlanStep(), 900);
        UNIT_ASSERT(guard->MayStartScanAt(Step(900), ssPathId));
        UNIT_ASSERT(!guard->MayStartScanAt(Step(850), ssPathId));

        auto holders = guard->BuildSnapshotHolders();
        UNIT_ASSERT(holders);
        UNIT_ASSERT_VALUES_EQUAL(holders->GetMinSnapshotForNewReads().GetPlanStep(), 900);
    }

    Y_UNIT_TEST(RegistryGuardWithoutBorderUsesActiveSnapshots) {
        const auto longTxConfig = MakeExplicitLongTxConfig();
        const ui64 schemeShardId = 123;
        NOlap::NTest::TTestPathIdTranslator translator;
        const auto internalPathId = TInternalPathId::FromRawValue(7);
        const auto ssPathId = TSchemeShardLocalPathId::FromRawValue(70);
        translator.Add(internalPathId, { ssPathId });

        auto registry = CreateSnapshotRegistry(std::nullopt, { { TTableId(schemeShardId, ssPathId.GetRawValue(), 0), TRowVersion(10, 1) } });

        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, translator, registry, longTxConfig);

        // default delay = 120 + 10 + 30 + 10 = 170 seconds, no border -> effective min step = 30000
        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads().GetPlanStep(), 30000);
        UNIT_ASSERT(guard->MayStartScanAt(Step(30000), ssPathId));
        UNIT_ASSERT(guard->MayStartScanAt(Step(10), ssPathId));
        UNIT_ASSERT(!guard->MayStartScanAt(Step(9), ssPathId));

        auto holders = guard->BuildSnapshotHolders();
        UNIT_ASSERT(holders);
        UNIT_ASSERT_VALUES_EQUAL(holders->GetMinSnapshotForNewReads().GetPlanStep(), 30000);
    }

    Y_UNIT_TEST(LastCleanupSnapshotIsRespectedForLocalGuard) {
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csControllerGuard->SetOverrideMaxReadStaleness(TDuration::MilliSeconds(250));

        auto tracker = MakeTracker();
        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot(900, 0);
        auto guard = CreateLocalScanSnapshotGuard(/*passedStep*/ 1000, lastCleanupSnapshot, tracker);

        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads(), lastCleanupSnapshot);
        UNIT_ASSERT(!guard->MayStartScanAt(Step(850), TSchemeShardLocalPathId::FromRawValue(1)));
        UNIT_ASSERT(guard->MayStartScanAt(Step(900), TSchemeShardLocalPathId::FromRawValue(1)));
    }

    Y_UNIT_TEST(RegistryBorderPriorityOverLastCleanupSnapshot) {
        const auto longTxConfig = MakeExplicitLongTxConfig();
        const ui64 schemeShardId = 123;
        NOlap::NTest::TTestPathIdTranslator translator;
        auto registry = CreateSnapshotRegistry(TRowVersion(100, 0));
        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot(150, 0);
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, translator, registry, longTxConfig);

        // Border must win over cleanup watermark to avoid dropping active snapshots under the border.
        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads(), NOlap::TSnapshot(100, 0));
    }

    Y_UNIT_TEST(RegistryRespectsLastCleanupSnapshot) {
        const auto longTxConfig = MakeExplicitLongTxConfig();
        const ui64 schemeShardId = 123;
        NOlap::NTest::TTestPathIdTranslator translator;
        const auto internalPathId = TInternalPathId::FromRawValue(7);
        const auto ssPathId = TSchemeShardLocalPathId::FromRawValue(70);
        translator.Add(internalPathId, { ssPathId });

        auto registry = CreateSnapshotRegistry();
        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot(45000, 0);
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, translator, registry, longTxConfig);

        // No border: min snapshot should be max(serviceMinReadStep=30000, lastCleanupSnapshot=45000) = 45000.
        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads(), lastCleanupSnapshot);
        UNIT_ASSERT(guard->MayStartScanAt(Step(45000), ssPathId));
        UNIT_ASSERT(!guard->MayStartScanAt(Step(44999), ssPathId));

        auto holders = guard->BuildSnapshotHolders();
        UNIT_ASSERT(holders);
        UNIT_ASSERT_VALUES_EQUAL(holders->GetMinSnapshotForNewReads(), lastCleanupSnapshot);
    }
}

}   // namespace NKikimr::NColumnShard
