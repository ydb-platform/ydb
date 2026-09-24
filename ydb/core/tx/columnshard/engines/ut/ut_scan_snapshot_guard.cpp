#include "test_path_id_translator.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/scan_snapshot_guard.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard {

Y_UNIT_TEST_SUITE(TScanSnapshotGuardTests) {
    NOlap::TSnapshot Step(const ui64 planStep) {
        return NOlap::TSnapshot(planStep, 1);
    }

    TInFlightReadsTracker MakeTracker() {
        return TInFlightReadsTracker(nullptr, std::make_shared<TRequestsTracerCounters>());
    }

    class TTestReadMetadata: public NOlap::NReader::TReadMetadataBase {
    public:
        explicit TTestReadMetadata(const NOlap::TSnapshot& snapshot)
            : TReadMetadataBase(
                  nullptr, NOlap::NReader::ERequestSorting::ASC, NOlap::TProgramContainer(), nullptr, snapshot, nullptr, /*tabletId*/ 1)
        {
        }

        std::unique_ptr<NOlap::NReader::TScanIteratorBase> StartScan(
            const std::shared_ptr<NOlap::NReader::TReadContext>& /*readContext*/) const override {
            return nullptr;
        }

        std::vector<NOlap::TNameTypeInfo> GetKeyYqlSchema() const override {
            return {};
        }
    };

    ui64 StartScan(
        TInFlightReadsTracker & tracker, const NOlap::TSnapshot& snapshot, const std::optional<TInternalPathId> pathId = std::nullopt) {
        const ui64 cookie = tracker.AddInFlightRequest(std::make_shared<TTestReadMetadata>(snapshot), nullptr, pathId);
        tracker.AddScanActorId(cookie, TActorId(1, cookie));
        return cookie;
    }

    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> CreateSnapshotRegistry(const std::optional<TRowVersion>& border = std::nullopt,
        const std::vector<std::pair<NKikimr::TTableId, TRowVersion>>& snapshots = {}, const TInstant oldestCollectionTime = TInstant::Zero()) {
        auto registryBuilder = CreateImmutableSnapshotRegistryBuilder();
        if (border) {
            registryBuilder->SetSnapshotBorder(*border);
        }
        registryBuilder->SetOldestCollectionTime(oldestCollectionTime);
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
        config.SetMaxClockSkewMs(5000);
        return config;
    }

    ui64 FreshnessMarginMs(const NKikimrConfig::TLongTxServiceConfig& config) {
        return TDuration::Seconds(config.GetLocalSnapshotPromotionTimeSeconds()).MilliSeconds() + config.GetMaxClockSkewMs();
    }

    Y_UNIT_TEST(LocalGuardSmoke) {
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csControllerGuard->SetOverrideMaxReadStaleness(TDuration::MilliSeconds(250));

        auto tracker = MakeTracker();
        NOlap::NTest::TTestPathIdTranslator translator;
        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();
        auto guard = CreateLocalScanSnapshotGuard(/*passedStep*/ 1000, lastCleanupSnapshot, tracker, translator);

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

        // Collection time well newer than the border, so the border caps the floor.
        const ui64 marginMs = FreshnessMarginMs(longTxConfig);
        auto registry = CreateSnapshotRegistry(TRowVersion(900, 0), {}, TInstant::MilliSeconds(30000 + marginMs));

        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();
        auto tracker = MakeTracker();
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, tracker, translator, registry, longTxConfig);

        // border = 900 is older than the freshness floor -> effective min step = 900
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

        auto registry = CreateSnapshotRegistry(
            std::nullopt, { { TTableId(schemeShardId, ssPathId.GetRawValue(), 0), TRowVersion(10, 1) } }, TInstant::MilliSeconds(155000));

        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();
        auto tracker = MakeTracker();
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, tracker, translator, registry, longTxConfig);

        // No border: floor = OldestCollectionTime(155000) - margin(125000) = 30000.
        const ui64 marginMs = FreshnessMarginMs(longTxConfig);
        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads().GetPlanStep(), 155000 - marginMs);
        UNIT_ASSERT(guard->MayStartScanAt(Step(30000), ssPathId));
        UNIT_ASSERT(guard->MayStartScanAt(Step(10), ssPathId));
        UNIT_ASSERT(!guard->MayStartScanAt(Step(9), ssPathId));

        auto holders = guard->BuildSnapshotHolders();
        UNIT_ASSERT(holders);
        UNIT_ASSERT_VALUES_EQUAL(holders->GetMinSnapshotForNewReads().GetPlanStep(), 30000);
    }

    Y_UNIT_TEST(RegistryGuardPinsSnapshotsOfActiveLocalScans) {
        // The registry has nothing for the table: the KQP side of the scan is gone, the scan actor on the tablet is not.
        const auto longTxConfig = MakeExplicitLongTxConfig();
        const ui64 schemeShardId = 123;
        NOlap::NTest::TTestPathIdTranslator translator;
        const auto internalPathId = TInternalPathId::FromRawValue(7);
        const auto ssPathId = TSchemeShardLocalPathId::FromRawValue(70);
        translator.Add(internalPathId, { ssPathId });
        const ui64 marginMs = FreshnessMarginMs(longTxConfig);
        auto registry = CreateSnapshotRegistry(std::nullopt, {}, TInstant::MilliSeconds(30000 + marginMs));
        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();

        // Visible from step 1000 until removed at step 20000, both below the floor of 30000.
        const auto portion = NOlap::NTest::MakeTestCompactedPortion(internalPathId, 1, 10, 19, 10, Step(1000), Step(20000));
        const auto couldUsePortion = [&](const TInFlightReadsTracker& tracker) {
            auto guard = CreateRegistryScanSnapshotGuard(
                /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, tracker, translator, registry, longTxConfig);
            UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads().GetPlanStep(), 30000);
            return guard->BuildSnapshotHolders()->CouldUsePortion(portion);
        };

        auto tracker = MakeTracker();
        UNIT_ASSERT(!couldUsePortion(tracker));

        // A running scan that sees the portion keeps it.
        const ui64 scanCookie = StartScan(tracker, Step(5000));
        UNIT_ASSERT(couldUsePortion(tracker));

        // A running scan that cannot see the portion does not keep it.
        auto blindTracker = MakeTracker();
        StartScan(blindTracker, Step(25000));
        UNIT_ASSERT(!couldUsePortion(blindTracker));

        // Once the scan has reported that it finished, the portion is free again at once, although the tracker still
        // remembers the snapshot until the used-snapshot livetime expires.
        Y_UNUSED(tracker.ExtractInFlightRequest(scanCookie, nullptr, TInstant::Now()));
        UNIT_ASSERT(tracker.HasLiveSnapshot(Step(5000)));
        UNIT_ASSERT(!couldUsePortion(tracker));
    }

    Y_UNIT_TEST(RegistryGuardScopesLocalScansToTheirTable) {
        const auto longTxConfig = MakeExplicitLongTxConfig();
        const ui64 schemeShardId = 123;
        NOlap::NTest::TTestPathIdTranslator translator;
        const auto scannedPathId = TInternalPathId::FromRawValue(7);
        const auto otherPathId = TInternalPathId::FromRawValue(8);
        translator.Add(scannedPathId, { TSchemeShardLocalPathId::FromRawValue(70) });
        translator.Add(otherPathId, { TSchemeShardLocalPathId::FromRawValue(80) });
        const ui64 marginMs = FreshnessMarginMs(longTxConfig);
        auto registry = CreateSnapshotRegistry(std::nullopt, {}, TInstant::MilliSeconds(30000 + marginMs));

        const auto scannedPortion = NOlap::NTest::MakeTestCompactedPortion(scannedPathId, 1, 10, 19, 10, Step(1000), Step(20000));
        const auto otherPortion = NOlap::NTest::MakeTestCompactedPortion(otherPathId, 2, 10, 19, 10, Step(1000), Step(20000));
        const auto holders = [&](const TInFlightReadsTracker& tracker) {
            return CreateRegistryScanSnapshotGuard(
                /*passedStep*/ 200000, schemeShardId, NOlap::TSnapshot::Zero(), tracker, translator, registry, longTxConfig)
                ->BuildSnapshotHolders();
        };

        auto tracker = MakeTracker();
        StartScan(tracker, Step(5000), scannedPathId);
        UNIT_ASSERT(holders(tracker)->CouldUsePortion(scannedPortion));
        UNIT_ASSERT(!holders(tracker)->CouldUsePortion(otherPortion));

        // A scan without a table of its own pins every table.
        auto tabletWideTracker = MakeTracker();
        StartScan(tabletWideTracker, Step(5000));
        UNIT_ASSERT(holders(tabletWideTracker)->CouldUsePortion(scannedPortion));
        UNIT_ASSERT(holders(tabletWideTracker)->CouldUsePortion(otherPortion));
    }

    Y_UNIT_TEST(LastCleanupSnapshotIsRespectedForLocalGuard) {
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csControllerGuard->SetOverrideMaxReadStaleness(TDuration::MilliSeconds(250));

        auto tracker = MakeTracker();
        NOlap::NTest::TTestPathIdTranslator translator;
        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot(900, 0);
        auto guard = CreateLocalScanSnapshotGuard(/*passedStep*/ 1000, lastCleanupSnapshot, tracker, translator);

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
        auto tracker = MakeTracker();
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, tracker, translator, registry, longTxConfig);

        // Border must win over cleanup watermark to avoid dropping active snapshots under the border.
        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads(), NOlap::TSnapshot(100, 0));
    }

    Y_UNIT_TEST(RegistryGuardRespectsCopySnapshot) {
        const auto longTxConfig = MakeExplicitLongTxConfig();
        const ui64 schemeShardId = 123;
        NOlap::NTest::TTestPathIdTranslator translator;
        const auto internalPathId = TInternalPathId::FromRawValue(7);
        const auto roTablePathId = TSchemeShardLocalPathId::FromRawValue(70);
        translator.Add(internalPathId, { roTablePathId });

        const ui64 marginMs = FreshnessMarginMs(longTxConfig);
        const auto oldestCollectionTimeMs = 30000;
        const auto copySnapshot = Step(oldestCollectionTimeMs - 2000);
        const auto border = TRowVersion(oldestCollectionTimeMs - 1000, 0);
        translator.SetCopyVersion(roTablePathId, copySnapshot);

        auto registry = CreateSnapshotRegistry(border, {}, TInstant::MilliSeconds(oldestCollectionTimeMs + marginMs));

        const NOlap::TSnapshot lastCleanupSnapshot = NOlap::TSnapshot::Zero();
        auto tracker = MakeTracker();
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, tracker, translator, registry, longTxConfig);

        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads().GetPlanStep(), border.Step);
        UNIT_ASSERT(guard->MayStartScanAt(copySnapshot, roTablePathId));
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
        auto tracker = MakeTracker();
        auto guard = CreateRegistryScanSnapshotGuard(
            /*passedStep*/ 200000, schemeShardId, lastCleanupSnapshot, tracker, translator, registry, longTxConfig);

        // OldestCollectionTime unset -> serviceMinReadStep=0; no border:
        // min snapshot should be max(0, lastCleanupSnapshot=45000) = 45000.
        UNIT_ASSERT_VALUES_EQUAL(guard->GetMinSnapshotForNewReads(), lastCleanupSnapshot);
        UNIT_ASSERT(guard->MayStartScanAt(Step(45000), ssPathId));
        UNIT_ASSERT(!guard->MayStartScanAt(Step(44999), ssPathId));

        auto holders = guard->BuildSnapshotHolders();
        UNIT_ASSERT(holders);
        UNIT_ASSERT_VALUES_EQUAL(holders->GetMinSnapshotForNewReads(), lastCleanupSnapshot);
    }
}

}   // namespace NKikimr::NColumnShard
