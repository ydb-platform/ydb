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

ui64 WriteValueOnShard(TScanContext& context, const ui64 key, const ui32 messageValue) {
    TTestBlobOptions blobOptions;
    blobOptions.SameValueColumns.insert("message");
    blobOptions.SameValue = messageValue;
    const TString data = MakeTestBlob({ key, key + 1 }, context.YdbSchema, blobOptions);
    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(context.Runtime, context.Sender, context.WriteId++, context.TableId, data, context.YdbSchema, true, &writeIds));
    const ui64 txId = context.TxId++;
    const auto planStep = ProposeCommit(context.Runtime, context.Sender, txId, writeIds);
    PlanCommit(context.Runtime, context.Sender, planStep, txId);
    return planStep.Val();
}

void AssertScanHasSingleMessageValue(TScanContext& context, const NOlap::TSnapshot& snapshot, const TString& expectedMessage) {
    TShardReader reader(context.Runtime, TTestTxConfig::TxTablet0, context.TableId, snapshot);
    reader.SetReplyColumnIds(TTestSchema::GetColumnIds(context.YdbSchema, { "timestamp", "message" }));
    auto result = reader.ReadAll();
    UNIT_ASSERT(!reader.IsError());
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->num_rows(), 1);
    auto messageColumn = std::dynamic_pointer_cast<arrow::StringArray>(result->GetColumnByName("message"));
    UNIT_ASSERT(messageColumn);
    UNIT_ASSERT_VALUES_EQUAL(TString(messageColumn->GetString(0)), expectedMessage);
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

void RunCleanupAndWait(TScanContext& context) {
    auto* controller = dynamic_cast<TDefaultTestsController*>(NKikimr::NYDBTest::TControllers::GetColumnShardController().get());
    UNIT_ASSERT(controller);
    Wakeup(context.Runtime, context.Sender, TTestTxConfig::TxTablet0);
    // Cleanup may legitimately have nothing to do, so WaitCleaning can return false.
    // We still force processing of the periodic wakeup and give cleanup cycle time to run.
    context.Runtime.SimulateSleep(TDuration::Seconds(1));
    Y_UNUSED(controller->WaitCleaning(TDuration::Seconds(1), &context.Runtime));
}

}   // namespace

Y_UNIT_TEST_SUITE(TScanSnapshotGuardIntegration) {
    Y_UNIT_TEST(LegacySnapshotGuard) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        csControllerGuard->SetOverrideMaxReadStaleness(TDuration::Seconds(5));

        TScanContext context;
        BootColumnShard(context, /*enableSnapshotsLocking*/ false);
        Y_UNUSED(SetupSchema(context.Runtime, context.Sender, context.TableId));

        const ui64 write1Step = WriteValueOnShard(context, /*key*/ 1, /*message*/ 1);
        const NOlap::TSnapshot sharedSnapshot(write1Step, 2000);
        const NOlap::TSnapshot oldInactiveSnapshot(write1Step, 2001);

        // 1) Younger than minSnapshotForScans -> allowed, sees old value
        AssertScanHasSingleMessageValue(context, sharedSnapshot, "1");

        // Make the current time to the last moment when sharedSnapshot may start
        context.Runtime.SimulateSleep(TDuration::Seconds(3));
        Y_UNUSED(UpdateSnapshotOnShard(context));

        // Start a scan with snapshot=sharedSnapshot at the last moment when it is still fresh.
        TShardReader activeReader = StartActiveScan(context, sharedSnapshot);

        // Write a newer version for the same key.
        Y_UNUSED(WriteValueOnShard(context, /*key*/ 1, /*message*/ 3));

        // Make sharedSnapshot older than minSnapshotForScans.
        context.Runtime.SimulateSleep(TDuration::Seconds(2));
        const ui64 passedStep = UpdateSnapshotOnShard(context);
        UNIT_ASSERT(passedStep > 5000);
        RunCleanupAndWait(context);

        // 2) Older than minSnapshotForScans and not active -> rejected.
        AssertScanRejectedAsTooOld(context, oldInactiveSnapshot);

        // 3) Older than minSnapshotForScans, but active same snapshot exists -> allowed, sees old value.
        AssertScanHasSingleMessageValue(context, sharedSnapshot, "1");
        // 4) Fresh snapshot -> allowed, sees new value.
        AssertScanHasSingleMessageValue(context, NOlap::TSnapshot(passedStep, 0), "3");

        // Finalize active scan reader.
        activeReader.Ack();
        activeReader.ContinueReadAll();
    }

    Y_UNIT_TEST(RegistrySnapshotGuardWithBorder) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TScanContext context;
        BootColumnShard(context, /*enableSnapshotsLocking*/ true);
        Y_UNUSED(SetupSchema(context.Runtime, context.Sender, context.TableId));

        auto& longTxConfig = context.Runtime.GetAppData(0).LongTxServiceConfig;
        longTxConfig.SetLocalSnapshotPromotionTimeSeconds(1);
        longTxConfig.SetSnapshotsExchangeIntervalSeconds(2);
        longTxConfig.SetSnapshotsRegistryUpdateIntervalSeconds(3);

        const ui64 write1Step = WriteValueOnShard(context, /*key*/ 1, /*message*/ 1);
        const TRowVersion border(write1Step, 2000);
        ConfigureRegistry(context, border);

        const NOlap::TSnapshot snapshotAtBorder(border.Step, border.TxId);
        AssertScanHasSingleMessageValue(context, snapshotAtBorder, "1");

        // Delay > (1 + 2 + 3 + 10)s so service cutoff becomes newer than border.
        // This makes border the effective min snapshot for new reads.
        context.Runtime.SimulateSleep(TDuration::Seconds(17));
        const ui64 write2Step = WriteValueOnShard(context, /*key*/ 1, /*message*/ 3);
        UNIT_ASSERT(write2Step > write1Step);
        RunCleanupAndWait(context);

        const NOlap::TSnapshot snapshotYoungerThanBorder(write2Step, 3000);
        const NOlap::TSnapshot snapshotOlderThanBorder(border.Step, border.TxId - 1);

        // Snapshot at border sees old value.
        AssertScanHasSingleMessageValue(context, snapshotAtBorder, "1");
        // Younger snapshot sees new value.
        AssertScanHasSingleMessageValue(context, snapshotYoungerThanBorder, "3");

        // Snapshot < border should be rejected.
        AssertScanRejectedAsTooOld(context, snapshotOlderThanBorder);
    }

    Y_UNIT_TEST(RegistrySnapshotGuardWithActiveSnapshot) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
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

        const ui64 write1Step = WriteValueOnShard(context, /*key*/ 1, /*message*/ 1);
        const NOlap::TSnapshot activeSnapshot(write1Step, 2000);
        AssertScanHasSingleMessageValue(context, activeSnapshot, "1");
        ConfigureRegistry(context, std::nullopt, { TRowVersion(activeSnapshot.GetPlanStep(), activeSnapshot.GetTxId()) });

        Y_UNUSED(WriteValueOnShard(context, /*key*/ 1, /*message*/ 3));
        context.Runtime.SimulateSleep(TDuration::Seconds(20));
        const ui64 passedStep = UpdateSnapshotOnShard(context);
        UNIT_ASSERT(passedStep > delayMs + 10);
        const ui64 minStep = passedStep - delayMs;
        UNIT_ASSERT(minStep > activeSnapshot.GetPlanStep() + 1);
        RunCleanupAndWait(context);

        // Exactly at minSnapshotForNewScans (last allowed moment) -> allowed, sees new value.
        AssertScanHasSingleMessageValue(context, NOlap::TSnapshot(minStep, 0), "3");
        // Older than minSnapshotForNewScans, but active in registry -> allowed, sees old value.
        AssertScanHasSingleMessageValue(context, activeSnapshot, "1");
        // One step older than minSnapshotForNewScans and not active -> rejected.
        AssertScanRejectedAsTooOld(context, NOlap::TSnapshot(minStep - 1, 2001));
    }
}

}   // namespace NKikimr
