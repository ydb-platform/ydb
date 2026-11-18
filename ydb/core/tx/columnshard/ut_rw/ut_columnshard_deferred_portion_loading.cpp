#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/data_events/events.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

namespace {

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

const ui64 TableId = 1;
const TDuration NoAnswerWait = TDuration::Seconds(1);

struct TShardOptions {
    ui32 TableCount = 1;
    bool InMemMetadata = false;
};

// Every table gets three committed portions, one per write: rows 0..99, 100..199, 200..299; batches of one portion each after a reboot
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
        Runtime.GetAppData().ColumnShardConfig.MutableDeferredPortionLoadingSettings()->SetEnabled(true);
        Runtime.GetAppData().ColumnShardConfig.MutableDeferredPortionLoadingSettings()->SetPortionsPerBatch(1);
        Sender = Runtime.AllocateEdgeActor();
        CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime.DispatchEvents(options);
        CreateTables(shardOptions);
        for (ui32 i = 0; i < shardOptions.TableCount; ++i) {
            for (ui64 from = 0; from < 300; from += 100) {
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
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardDeferredPortionLoading) {
    Y_UNIT_TEST(LoadsEveryPortionInBatches) {
        TShardWithPortions shard;
        shard.Reboot();
        shard.WaitBatchesLoaded(3);
        UNIT_ASSERT(shard.Engine().AreAllPortionsLoaded(std::nullopt));
        UNIT_ASSERT_VALUES_EQUAL(shard.Engine().GetTables().begin()->second->GetPortions().size(), 3);

        auto reader = shard.StartScan({ 0, 299 });
        UNIT_ASSERT(reader->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*reader), 300);
    }

    // the newest portion comes first, so a scan of recent keys does not wait for the rest
    Y_UNIT_TEST(ScanOfNewestKeysPassesAfterFirstBatch) {
        TShardWithPortions shard;
        shard.AllowBatches(0);
        shard.Reboot();
        auto newest = shard.StartScan({ 299, 299 });
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
        auto reader = shard.StartScan({ 0, 299 });
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
        auto reader = shard.StartScan({ 0, 299 });
        UNIT_ASSERT(reader->WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(shard.ReadRows(*reader), 300);
        UNIT_ASSERT_VALUES_EQUAL(shard.Controller->GetPortionBatchesLoaded(), 0);
    }
}

}   // namespace NKikimr
