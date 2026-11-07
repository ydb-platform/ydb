#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

namespace {

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

const ui64 TableId = 1;
const ui64 LockId = 7;
const TDuration NoAnswerWait = TDuration::Seconds(1);

struct TShardOptions {
    ui32 TableCount = 1;
    bool InMemMetadata = false;
    TString CompactionPreset = "tiling++";
    TString TilingJson;
};

// Every table gets three committed portions, one per write: rows 0..99, 200..299, 400..499; batches of one portion each after a reboot,
// the newest first: after one batch the keys from 499 are loaded, after two from 299, after three everything
class TShardWithPortions {
public:
    NKikimr::NYDBTest::TControllers::TGuard<TDefaultTestsController> Controller =
        NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;
    NOlap::TSnapshot Snapshot = NOlap::TSnapshot::Zero();
    ui64 TxId = 100;
    const TestTableDescription Table;

    explicit TShardWithPortions(const TShardOptions& shardOptions = {}) {
        Controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        TTester::Setup(Runtime);
        Runtime.GetAppData().ColumnShardConfig.SetDefaultCompactionPreset(shardOptions.CompactionPreset);
        Runtime.GetAppData().ColumnShardConfig.MutableDeferredPortionLoadingSettings()->SetEnabled(true);
        Runtime.GetAppData().ColumnShardConfig.MutableDeferredPortionLoadingSettings()->SetPortionsPerBatch(1);
        Sender = Runtime.AllocateEdgeActor();
        CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime.DispatchEvents(options);
        CreateTables(shardOptions);
        for (ui32 i = 0; i < shardOptions.TableCount; ++i) {
            for (ui64 from = 0; from < 600; from += 200) {
                WriteAndCommit(TableId + i, { from, from + 100 });
            }
        }
    }

    void CreateTables(const TShardOptions& shardOptions) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* init = tx.MutableInitShard();
        init->SetOwnerPath("/Root/olap");
        init->SetOwnerPathId(TableId);
        for (ui32 i = 0; i < shardOptions.TableCount; ++i) {
            auto* table = init->AddTables();
            TSchemeShardLocalPathId::FromRawValue(TableId + i).ToProto(*table);
            TTestSchema::InitSchema(Table.Schema, Table.Pk, {}, table->MutableSchema());
            if (shardOptions.InMemMetadata) {
                table->MutableSchema()->MutableOptions()->MutableMetadataManagerConstructor()->SetClassName("in_mem");
            }
            if (shardOptions.TilingJson) {
                auto* planner = table->MutableSchema()->MutableOptions()->MutableCompactionPlannerConstructor();
                planner->SetClassName("tiling++");
                planner->MutableTiling()->SetJson(shardOptions.TilingJson);
            }
        }
        Y_UNUSED(SetupSchema(Runtime, Sender, tx.SerializeAsString(), 10));
    }

    void WriteAndCommit(const ui64 tableId, const std::pair<ui64, ui64> rows) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(Runtime, Sender, TxId, tableId, MakeTestBlob(rows, Table.Schema), Table.Schema, true, &writeIds));
        const TPlanStep planStep = ProposeCommit(Runtime, Sender, TxId, writeIds);
        PlanCommit(Runtime, Sender, planStep, TxId);
        Snapshot = NOlap::TSnapshot(planStep, Max<ui64>());
        ++TxId;
    }

    void Reboot() {
        RebootTablet(Runtime, TTestTxConfig::TxTablet0, Sender);
    }

    void AllowBatches(const std::optional<ui32> limit) {
        Controller->SetPortionBatchesLimit(limit);
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvPrivate::TEvPeriodicWakeup(true));
    }

    void WaitBatchesLoaded(const ui32 count) {
        Runtime.WaitFor("batches loaded", [&] {
            return Controller->GetPortionBatchesLoaded() >= count;
        }, TDuration::Seconds(30));
    }

    const NOlap::TColumnEngineForLogs& Engine() {
        return Controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>();
    }

    std::unique_ptr<TShardReader> StartScan(const std::pair<ui64, ui64> rows) {
        auto reader = std::make_unique<TShardReader>(Runtime, TTestTxConfig::TxTablet0, TableId, Snapshot);
        reader->SetReplyColumnIds(Table.GetColumnIds({ "timestamp" }));
        reader->AddRange(MakeTestRange(rows, true, true, Table.Pk));
        reader->StartScanner();
        return reader;
    }

    ui64 ReadRows(TShardReader& reader) {
        reader.Ack();
        const auto rows = reader.ContinueReadAll();
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        return rows ? rows->num_rows() : 0;
    }

    void StartWrite(const std::pair<ui64, ui64> rows, const NEvWrite::EModificationType modificationType = NEvWrite::EModificationType::Upsert,
        const ui64 lockId = 1) {
        UNIT_ASSERT(
            WriteData(Runtime, Sender, 2, TableId, MakeTestBlob(rows, Table.Schema), Table.Schema, false, nullptr, modificationType, lockId));
    }

    void StartWriteWithTimeout(const std::pair<ui64, ui64> rows, const ui64 lockId) {
        UNIT_ASSERT(WriteData(Runtime, Sender, TTestTxConfig::TxTablet0, 2, TableId, MakeTestBlob(rows, Table.Schema), Table.Schema, nullptr,
            NEvWrite::EModificationType::Upsert, lockId, TDuration::Seconds(1)));
    }

    void StartSnapshotIsolatedUpsert(const std::pair<ui64, ui64> rows) {
        auto write = std::make_unique<NEvents::TDataEvents::TEvWrite>(2, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        write->SetLockId(LockId, 1);
        write->Record.SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
        write->Record.MutableMvccSnapshot()->SetStep(Snapshot.GetPlanStep());
        write->Record.MutableMvccSnapshot()->SetTxId(Snapshot.GetTxId());
        auto& operation = write->AddOperation(
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, TTableId(0, TableId, 1), {}, 0, NKikimrDataEvents::FORMAT_ARROW);
        *operation.MutablePayloadSchema() = NArrow::SerializeSchema(*NArrow::MakeArrowSchema(Table.Schema));
        NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite> writer(*write);
        writer.AddDataToPayload(MakeTestBlob(rows, Table.Schema));
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, write.release());
    }

    bool IsWriteAnswered() {
        TAutoPtr<IEventHandle> handle;
        return Runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(handle, NoAnswerWait);
    }

    ui32 WaitWriteStatus() {
        return WaitWriteResult(Runtime, TTestTxConfig::TxTablet0);
    }

    // the gate's own retry is a scheduled wakeup, which the test runtime drops
    void ExpireWaitingWrites() {
        Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvents::TEvWakeup(1));
    }

    ui64 GetSimpleCounter(const TString& name) {
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvTablet::TEvGetCounters());
        const auto ev = Runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(Sender);
        for (const auto& counter : ev->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
            if (counter.GetName() == name) {
                return counter.GetValue();
            }
        }
        UNIT_FAIL("counter " << name << " not found");
        return 0;
    }

    void NotifyLockGone() {
        ForwardToTablet(
            Runtime, TTestTxConfig::TxTablet0, Sender, new NLongTxService::TEvLongTxService::TEvLockStatus(LockId, Runtime.GetNodeId(),
                                                           NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND));
    }

    const NColumnShard::TOperationsManager& Operations() {
        return Controller->GetTheOnlyShard()->GetOperationsManager();
    }

    void AddSchemaVersion(const ui32 version) {
        NKikimrTxColumnShard::TSchemaTxBody tx;
        UNIT_ASSERT(tx.ParseFromString(TTestSchema::AlterTableTxBody(TableId, true, version, Table.Schema, Table.Pk, {})));
        tx.MutableAlterTable()->MutableSchema()->SetVersion(version);
        Y_UNUSED(SetupSchema(Runtime, Sender, tx.SerializeAsString(), 200 + version));
    }

    void SendPeriodicWakeup() {
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvPrivate::TEvPeriodicWakeup(true));
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardDeferredPortionLoading) {
    Y_UNIT_TEST(LoadsEveryPortionInBatches) {
        TShardWithPortions shard;
        shard.Reboot();
        shard.WaitBatchesLoaded(3);
        UNIT_ASSERT(shard.Engine().AreAllPortionsLoaded(std::nullopt));
        UNIT_ASSERT_VALUES_EQUAL(shard.Engine().GetTables().begin()->second->GetPortions().size(), 3);

        auto reader = shard.StartScan({ 0, 499 });
        UNIT_ASSERT(reader->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*reader), 300);
    }

    // the newest portion comes first, so a scan of recent keys does not wait for the rest
    Y_UNIT_TEST(ScanOfNewestKeysPassesAfterFirstBatch) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        auto newest = shard.StartScan({ 499, 499 });
        UNIT_ASSERT(!newest->WaitScannerAnswer(NoAnswerWait));

        shard.AllowBatches(1);
        UNIT_ASSERT(newest->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*newest), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.Controller->GetPortionBatchesLoaded(), 1);
    }

    Y_UNIT_TEST(ScanOfOldestKeysWaitsForLastBatch) {
        TShardWithPortions shard;
        shard.AllowBatches(2);
        shard.Reboot();
        shard.WaitBatchesLoaded(2);
        auto oldest = shard.StartScan({ 0, 10 });
        UNIT_ASSERT(!oldest->WaitScannerAnswer(NoAnswerWait));

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT(oldest->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*oldest), 11);
    }

    Y_UNIT_TEST(WriteOfOldestKeysWaitsForLastBatch) {
        TShardWithPortions shard;
        shard.AllowBatches(2);
        shard.Reboot();
        shard.WaitBatchesLoaded(2);
        UNIT_ASSERT(WriteData(shard.Runtime, shard.Sender, 2, TableId, MakeTestBlob({ 50, 60 }, shard.Table.Schema), shard.Table.Schema, false,
            nullptr, NEvWrite::EModificationType::Upsert, 1));
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!shard.Runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(handle, NoAnswerWait));

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitWriteResult(shard.Runtime, TTestTxConfig::TxTablet0), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(RestartInTheMiddleLoadsEverything) {
        TShardWithPortions shard;
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);

        shard.Controller->SetPortionBatchesLimit(std::nullopt);
        shard.Reboot();
        shard.WaitBatchesLoaded(4);
        UNIT_ASSERT(shard.Engine().AreAllPortionsLoaded(std::nullopt));
        auto reader = shard.StartScan({ 0, 499 });
        UNIT_ASSERT(reader->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*reader), 300);
    }

    // a prepared transaction's portion is loaded in TTxInit and met again in a batch
    Y_UNIT_TEST(UncommittedPortionLoadedAtStartIsNotLoadedTwice) {
        TShardWithPortions shard;
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(shard.Runtime, shard.Sender, shard.TxId, TableId, MakeTestBlob({ 1000, 1010 }, shard.Table.Schema),
            shard.Table.Schema, true, &writeIds));
        const ui64 txId = shard.TxId++;
        const TPlanStep planStep = ProposeCommit(shard.Runtime, shard.Sender, txId, writeIds);

        shard.Reboot();
        shard.WaitBatchesLoaded(4);
        UNIT_ASSERT(shard.Engine().AreAllPortionsLoaded(std::nullopt));
        UNIT_ASSERT_VALUES_EQUAL(shard.Engine().GetTables().begin()->second->GetInsertedPortions().size(), 1);

        PlanCommit(shard.Runtime, shard.Sender, planStep, txId);
        shard.Snapshot = NOlap::TSnapshot(planStep, Max<ui64>());
        auto reader = shard.StartScan({ 0, 1010 });
        UNIT_ASSERT(reader->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*reader), 310);
    }

    Y_UNIT_TEST(TablesLoadInTurn) {
        TShardWithPortions shard({ .TableCount = 2 });
        shard.AllowBatches(2);
        shard.Reboot();
        shard.WaitBatchesLoaded(2);
        UNIT_ASSERT_VALUES_EQUAL(shard.Engine().GetTables().size(), 2);
        for (const auto& [_, granule] : shard.Engine().GetTables()) {
            UNIT_ASSERT_VALUES_EQUAL(granule->GetPortions().size(), 1);
        }

        shard.AllowBatches(std::nullopt);
        shard.WaitBatchesLoaded(6);
        UNIT_ASSERT(shard.Engine().AreAllPortionsLoaded(std::nullopt));
        for (const auto& [_, granule] : shard.Engine().GetTables()) {
            UNIT_ASSERT_VALUES_EQUAL(granule->GetPortions().size(), 3);
        }
    }

    // the batches are held, so the scan answers only if TTxInit loaded every portion
    Y_UNIT_TEST(InMemMetadataLoadsEverythingAtStart) {
        TShardWithPortions shard({ .InMemMetadata = true });
        shard.AllowBatches(0);
        shard.Reboot();
        auto reader = shard.StartScan({ 0, 499 });
        UNIT_ASSERT(reader->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*reader), 300);
        UNIT_ASSERT_VALUES_EQUAL(shard.Controller->GetPortionBatchesLoaded(), 0);
    }

    Y_UNIT_TEST(ScanWaitingForLoadedPortionsIsCounted) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        auto reader = shard.StartScan({ 100, 200 });
        UNIT_ASSERT(!reader->WaitScannerAnswer(NoAnswerWait));
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/ScansWaitingLoadedPortions"), 1);

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT(reader->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/ScansWaitingLoadedPortions"), 0);
    }

    Y_UNIT_TEST(ScanWaitingForSnapshotIsCounted) {
        TShardWithPortions shard;
        TShardReader reader(
            shard.Runtime, TTestTxConfig::TxTablet0, TableId, NOlap::TSnapshot(shard.Snapshot.GetPlanStep() + 1000000, Max<ui64>()));
        reader.SetReplyColumnIds(shard.Table.GetColumnIds({ "timestamp" }));
        reader.AddRange(MakeTestRange({ 100, 200 }, true, true, shard.Table.Pk));
        reader.StartScanner();
        UNIT_ASSERT(!reader.WaitScannerAnswer(NoAnswerWait));
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/ScansWaitingSnapshot"), 1);
    }

    // an insert reads the rows it writes through an internal scan
    Y_UNIT_TEST(InsertBelowLoadedKeysWaits) {
        TShardWithPortions shard;
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);
        shard.StartWrite({ 300, 310 }, NEvWrite::EModificationType::Insert);
        UNIT_ASSERT(!shard.IsWriteAnswered());

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(WriteAboveLoadedKeysLands) {
        TShardWithPortions shard;
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);
        shard.StartWrite({ 600, 610 });
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(LcBucketsWriteBelowLoadedKeysLands) {
        TShardWithPortions shard({ .CompactionPreset = "lc-buckets" });
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);
        shard.StartWrite({ 100, 110 });
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    // its own read needs the rows, so it waits at the write gate, where it has a timeout
    Y_UNIT_TEST(LcBucketsInsertBelowLoadedKeysWaits) {
        TShardWithPortions shard({ .CompactionPreset = "lc-buckets" });
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);
        shard.StartWrite({ 300, 310 }, NEvWrite::EModificationType::Insert);
        UNIT_ASSERT(!shard.IsWriteAnswered());
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/WritesWaitingLoadedPortions"), 1);

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    // a blind write under snapshot isolation reads the rows committed after its snapshot to find conflicts
    Y_UNIT_TEST(LcBucketsSnapshotIsolatedUpsertBelowLoadedKeysWaits) {
        TShardWithPortions shard({ .CompactionPreset = "lc-buckets" });
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);
        shard.StartSnapshotIsolatedUpsert({ 300, 310 });
        UNIT_ASSERT(!shard.IsWriteAnswered());
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/WritesWaitingLoadedPortions"), 1);

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    // overlapping portions in the last level overload tiling++; compaction must clear it with the loading held
    Y_UNIT_TEST(CompactionClearsWriteOverloadWhileLoading) {
        TShardWithPortions shard({ .TilingJson = R"({"accumulator_portion_size_limit":1,"last_level_candidate_portions_overload":10})" });
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);
        shard.Runtime.GetAppData().FeatureFlags.SetEnableCompactionOverloadDetection(false);
        for (ui32 i = 0; i < 20; ++i) {
            shard.WriteAndCommit(TableId, { 600, 610 });
        }
        shard.Runtime.GetAppData().FeatureFlags.SetEnableCompactionOverloadDetection(true);
        shard.StartWrite({ 700, 710 });
        UNIT_ASSERT(!shard.IsWriteAnswered());

        shard.Controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        shard.SendPeriodicWakeup();
        shard.Runtime.WaitFor("compaction overload cleared", [&] {
            return !shard.Engine().GetTables().begin()->second->GetOptimizerPlanner().GetUsefulMetric().IsCritical();
        }, TDuration::Seconds(60));
        // the gate's own retry is a scheduled wakeup, which the test runtime drops
        ForwardToTablet(shard.Runtime, TTestTxConfig::TxTablet0, shard.Sender, new TEvents::TEvWakeup(1));
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(shard.Controller->GetPortionBatchesLoaded(), 1);
    }

    Y_UNIT_TEST(WriteWaitingForLoadedPortionsIsCounted) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        shard.StartWrite({ 100, 110 });
        UNIT_ASSERT(!shard.IsWriteAnswered());
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/WritesWaitingLoadedPortions"), 1);

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/WritesWaitingLoadedPortions"), 0);
    }

    // a held write still counts against the overload limit of the shard
    Y_UNIT_TEST(WriteWaitingForLoadedPortionsStaysInFlight) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        shard.StartWrite({ 100, 110 });
        UNIT_ASSERT(!shard.IsWriteAnswered());
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/WritesInFly"), 1);

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        UNIT_ASSERT_VALUES_EQUAL(shard.GetSimpleCounter("ColumnShard/WritesInFly"), 0);
    }

    Y_UNIT_TEST(WriteWaitingForLoadedPortionsTimesOutOverloaded) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        shard.StartWriteWithTimeout({ 100, 110 }, 1);
        UNIT_ASSERT(!shard.IsWriteAnswered());

        shard.ExpireWaitingWrites();
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
    }

    Y_UNIT_TEST(TimedOutNoTxWriteLeavesNoLock) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        shard.StartWriteWithTimeout({ 100, 110 }, 0);
        UNIT_ASSERT(!shard.IsWriteAnswered());

        shard.ExpireWaitingWrites();
        Y_UNUSED(shard.WaitWriteStatus());
        shard.Runtime.WaitFor("no lock of a not proposed transaction", [&] {
            return shard.Operations().GetLockIdsOfNotProposedTransactions().empty();
        }, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(WriteWaitingForLoadedPortionsFailsWhenLockAborted) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        shard.StartWrite({ 100, 110 }, NEvWrite::EModificationType::Upsert, LockId);
        UNIT_ASSERT(!shard.IsWriteAnswered());

        shard.NotifyLockGone();
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(LockOfWriteWaitingForLoadedPortionsIsAborted) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        shard.StartWrite({ 100, 110 }, NEvWrite::EModificationType::Upsert, LockId);
        UNIT_ASSERT(!shard.IsWriteAnswered());

        shard.NotifyLockGone();
        shard.Runtime.WaitFor("lock removed", [&] {
            return !shard.Operations().GetLockOptional(LockId);
        }, TDuration::Seconds(30));
    }

    Y_UNIT_TEST(WriteOfLockAbortedWhileParsedFails) {
        TShardWithPortions shard;
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);

        TBlockEvents<NOlap::TEvPassWriteGate> parsed(shard.Runtime);
        shard.StartWrite({ 600, 610 }, NEvWrite::EModificationType::Upsert, LockId);
        shard.Runtime.WaitFor("batch parsed", [&] {
            return !parsed.empty();
        }, TDuration::Seconds(30));

        shard.NotifyLockGone();
        shard.Runtime.WaitFor("lock marked for abort", [&] {
            const auto* lock = shard.Operations().GetLockOptional(LockId);
            return lock && lock->NeedsAborting();
        }, TDuration::Seconds(30));
        parsed.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(shard.WaitWriteStatus(), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
    }

    Y_UNIT_TEST(SysViewScanWaitsUntilEverythingIsLoaded) {
        TShardWithPortions shard;
        shard.AllowBatches(2);
        shard.Reboot();
        shard.WaitBatchesLoaded(2);

        TShardReader reader(shard.Runtime, TTestTxConfig::TxTablet0, TableId, shard.Snapshot);
        reader.SetTablePath("/.sys/primary_index_portion_stats");
        reader.SetReplyColumnIds({ NKikimr::NSysView::Schema::PrimaryIndexPortionStats::PortionId::ColumnId });
        reader.StartScanner();
        UNIT_ASSERT(!reader.WaitScannerAnswer(NoAnswerWait));

        shard.AllowBatches(std::nullopt);
        UNIT_ASSERT(reader.WaitScannerAnswer());
        Y_UNUSED(shard.ReadRows(reader));
    }

    // an unloaded portion may still use a version that no loaded portion uses
    Y_UNIT_TEST(SchemaCleanupWaitsUntilEverythingIsLoaded) {
        TShardWithPortions shard;
        shard.Runtime.GetAppData().FeatureFlags.SetEnableCSSchemasCollapsing(true);
        shard.AllowBatches(1);
        shard.Reboot();
        shard.WaitBatchesLoaded(1);

        shard.AddSchemaVersion(2);
        shard.AddSchemaVersion(3);
        shard.SendPeriodicWakeup();
        shard.Runtime.SimulateSleep(NoAnswerWait);
        UNIT_ASSERT_VALUES_EQUAL(shard.Controller->GetCleanupSchemasFinishedCounter().Val(), 0);

        shard.AllowBatches(std::nullopt);
        shard.WaitBatchesLoaded(3);
        shard.SendPeriodicWakeup();
        shard.Runtime.WaitFor("schemas cleaned", [&] {
            return shard.Controller->GetCleanupSchemasFinishedCounter().Val() > 0;
        }, TDuration::Seconds(30));
    }
}

}   // namespace NKikimr
