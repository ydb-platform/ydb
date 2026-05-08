#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;
using namespace Tests;

namespace {

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

struct TScanContext {
    TTestBasicRuntime Runtime;
    TActorId Sender;
    ui64 TableId = 1;
    ui64 WriteId = 1000;
    ui64 TxId = 1000;
    std::vector<TTestSchema::TTestColumn> YdbSchema = TTestSchema::YdbSchema();
};

void BootColumnShard(TScanContext& context, const bool enableSnapshotsLocking) {
    TTester::Setup(context.Runtime);
    context.Runtime.GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(enableSnapshotsLocking);
    context.Sender = context.Runtime.AllocateEdgeActor();

    CreateTestBootstrapper(context.Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        if (ev.GetTypeRewrite() != NNodeWhiteboard::TEvWhiteboard::EvTabletStateUpdate) {
            return false;
        }
        const auto* msg = ev.Get<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>();
        return msg->Record.GetTabletId() == TTestTxConfig::TxTablet0 && msg->Record.GetState() == NKikimrWhiteboard::TTabletStateInfo::Active;
    }, 1);
    context.Runtime.DispatchEvents(options);
}

void AssertScanAllowed(TScanContext& context, const NOlap::TSnapshot& snapshot) {
    TShardReader reader(context.Runtime, TTestTxConfig::TxTablet0, context.TableId, snapshot);
    reader.SetReplyColumnIds(TTestSchema::GetColumnIds(context.YdbSchema, { "timestamp", "message" }));
    reader.ReadAll();
    UNIT_ASSERT(!reader.IsError());
}

void AssertScanRejectedAsTooOld(TScanContext& context, const NOlap::TSnapshot& snapshot) {
    TShardReader reader(context.Runtime, TTestTxConfig::TxTablet0, context.TableId, snapshot);
    reader.SetReplyColumnIds(TTestSchema::GetColumnIds(context.YdbSchema, { "timestamp", "message" }));
    reader.ReadAll();
    UNIT_ASSERT(reader.IsError());
    UNIT_ASSERT(!reader.GetErrors().empty());
    UNIT_ASSERT_STRING_CONTAINS(reader.GetErrors().front().message(), "Snapshot too old");
}

TShardReader StartActiveScan(TScanContext& context, const NOlap::TSnapshot& snapshot) {
    TShardReader reader(context.Runtime, TTestTxConfig::TxTablet0, context.TableId, snapshot);
    reader.SetReplyColumnIds(TTestSchema::GetColumnIds(context.YdbSchema, { "timestamp", "message" }));
    UNIT_ASSERT(reader.InitializeScanner());
    UNIT_ASSERT(!reader.IsError());
    return reader;
}

ui64 UpdateSnapshotOnShard(TScanContext& context) {
    // In this UT, advancing shard step via schema tx can fail on schema-seqno checks.
    // A tiny write+commit is the stable way to move passed step forward.
    const TString data = MakeTestBlob({ 1, 2 }, context.YdbSchema);
    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(context.Runtime, context.Sender, context.WriteId++, context.TableId, data, context.YdbSchema, true, &writeIds));
    const ui64 txId = context.TxId++;
    const auto planStep = ProposeCommit(context.Runtime, context.Sender, txId, writeIds);
    PlanCommit(context.Runtime, context.Sender, planStep, txId);
    return planStep.Val();
}

void ConfigureRegistry(
    TScanContext& context, const std::optional<TRowVersion>& border = std::nullopt, const TVector<TRowVersion>& snapshots = {}) {
    auto registryBuilder = CreateImmutableSnapshotRegistryBuilder();
    if (border) {
        registryBuilder->SetSnapshotBorder(*border);
    }
    for (const auto& snapshot : snapshots) {
        registryBuilder->AddSnapshot({}, snapshot);
    }

    auto& appData = context.Runtime.GetAppData(0);
    if (!appData.SnapshotRegistryHolder) {
        appData.SnapshotRegistryHolder = CreateImmutableSnapshotRegistryHolder();
    }
    appData.SnapshotRegistryHolder->Set(std::move(*registryBuilder).Build());
}

}   // namespace

Y_UNIT_TEST_SUITE(TScanSnapshotGuardIntegration) {
    Y_UNIT_TEST(LegacySnapshotGuard) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        csControllerGuard->SetOverrideMaxReadStaleness(TDuration::Seconds(5));

        TScanContext context;
        BootColumnShard(context, /*enableSnapshotsLocking*/ false);
        const auto schemaStep = SetupSchema(context.Runtime, context.Sender, context.TableId);

        const ui64 schemaStepValue = schemaStep.Val();
        const NOlap::TSnapshot youngSnapshot(schemaStepValue, 100);
        const NOlap::TSnapshot sharedSnapshot(schemaStepValue, 101);
        const NOlap::TSnapshot oldInactiveSnapshot(schemaStepValue, 102);

        // 1) Younger than minSnapshotForScans -> allowed.
        AssertScanAllowed(context, youngSnapshot);

        // Make the current time to the last moment when sharedSnapshot may start
        context.Runtime.SimulateSleep(TDuration::Seconds(3));
        Y_UNUSED(UpdateSnapshotOnShard(context));

        // Start a scan with snapshot=sharedSnapshot at the last moment when it is still fresh.
        TShardReader activeReader = StartActiveScan(context, sharedSnapshot);

        // Make sharedSnapshot older than minSnapshotForScans.
        context.Runtime.SimulateSleep(TDuration::Seconds(2));
        Y_UNUSED(UpdateSnapshotOnShard(context));

        // 2) Older than minSnapshotForScans and not active -> rejected.
        AssertScanRejectedAsTooOld(context, oldInactiveSnapshot);

        // 3) Older than minSnapshotForScans, but active same snapshot exists -> allowed.
        AssertScanAllowed(context, sharedSnapshot);

        // Finalize active scan reader.
        activeReader.Ack();
        activeReader.ContinueReadAll();
    }

    Y_UNIT_TEST(RegistrySnapshotGuardWithBorder) {
        TScanContext context;
        BootColumnShard(context, /*enableSnapshotsLocking*/ true);
        const auto schemaStep = SetupSchema(context.Runtime, context.Sender, context.TableId);
        const ui64 schemaStepValue = schemaStep.Val();

        // Registry guard uses min(serviceCutoff, border). To test border behavior itself, pick a border
        // strictly older than any possible service cutoff in this scenario.
        const TRowVersion border(schemaStepValue - 1'000'000, Max<ui64>());
        ConfigureRegistry(context, border);

        const NOlap::TSnapshot snapshotYoungerThanBorder(border.Step + 1, 1);
        const NOlap::TSnapshot snapshotAtBorder(border.Step, border.TxId);
        const NOlap::TSnapshot snapshotOlderThanBorder(border.Step - 1, Max<ui64>());

        // Snapshot >= border should be allowed.
        AssertScanAllowed(context, snapshotYoungerThanBorder);
        AssertScanAllowed(context, snapshotAtBorder);

        // Snapshot < border should be rejected.
        AssertScanRejectedAsTooOld(context, snapshotOlderThanBorder);
    }

    Y_UNIT_TEST(RegistrySnapshotGuardWithoutBorderWithActiveSnapshot) {
        TScanContext context;
        BootColumnShard(context, /*enableSnapshotsLocking*/ true);
        Y_UNUSED(SetupSchema(context.Runtime, context.Sender, context.TableId));

        auto& longTxConfig = context.Runtime.GetAppData(0).LongTxServiceConfig;
        longTxConfig.SetLocalSnapshotPromotionTimeSeconds(1);
        longTxConfig.SetSnapshotsExchangeIntervalSeconds(2);
        longTxConfig.SetSnapshotsRegistryUpdateIntervalSeconds(3);
        const ui64 delayMs =
            TDuration::Seconds(longTxConfig.GetLocalSnapshotPromotionTimeSeconds() + longTxConfig.GetSnapshotsExchangeIntervalSeconds() +
                               longTxConfig.GetSnapshotsRegistryUpdateIntervalSeconds() + 10)
                .MilliSeconds();

        context.Runtime.SimulateSleep(TDuration::Seconds(20));
        const ui64 passedStep = UpdateSnapshotOnShard(context);
        UNIT_ASSERT(passedStep > delayMs + 10);
        const ui64 minStep = passedStep - delayMs;

        const NOlap::TSnapshot activeSnapshot(minStep - 2, Max<ui64>());
        ConfigureRegistry(context, std::nullopt, { TRowVersion(activeSnapshot.GetPlanStep(), activeSnapshot.GetTxId()) });

        // Exactly at minSnapshotForNewScans (last allowed moment) -> allowed.
        AssertScanAllowed(context, NOlap::TSnapshot(minStep, 0));
        // Older than minSnapshotForNewScans, but active in registry -> allowed.
        AssertScanAllowed(context, activeSnapshot);
        // One step older than minSnapshotForNewScans and not active -> rejected.
        AssertScanRejectedAsTooOld(context, NOlap::TSnapshot(minStep - 1, Max<ui64>()));
    }
}

}   // namespace NKikimr
