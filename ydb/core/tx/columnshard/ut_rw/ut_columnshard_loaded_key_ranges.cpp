#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/data_events/events.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

namespace {

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardLoadedKeyRanges) {
    const ui64 TableId = 1;
    const TDuration NoAnswerWait = TDuration::Seconds(1);

    // rows 0..999 of the test table, committed; returns the snapshot they are visible at
    NOlap::TSnapshot StartShardWithRows(TTestBasicRuntime & runtime, TActorId & sender) {
        TTester::Setup(runtime);
        sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        runtime.DispatchEvents(options);

        const TestTableDescription table;
        Y_UNUSED(SetupSchema(runtime, sender, TableId, table));
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, 1, TableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, true, &writeIds));
        const ui64 txId = 100;
        const TPlanStep planStep = ProposeCommit(runtime, sender, txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);
        return NOlap::TSnapshot(planStep, Max<ui64>());
    }

    void SetLoadedKeyRanges(TTestBasicRuntime & runtime, TActorId & sender,
        NKikimr::NYDBTest::TControllers::TGuard<TDefaultTestsController> & csControllerGuard, NOlap::TLoadedKeyRanges && loadedKeyRanges) {
        const TInternalPathId pathId = csControllerGuard->GetTheOnlyShard()->GetTablesManager().ResolveInternalPathIdVerified(
            TSchemeShardLocalPathId::FromRawValue(TableId), false);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvPrivate::TEvSetLoadedKeyRanges(pathId, std::move(loadedKeyRanges)));
    }

    NOlap::TLoadedKeyRanges LoadedFromRow(const ui64 row) {
        return NOlap::TLoadedKeyRanges::BuildFrom(MakeTestKey(row, TestTableDescription().Pk));
    }

    ui64 GetSimpleCounter(TTestBasicRuntime & runtime, TActorId & sender, const TString& name) {
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvTablet::TEvGetCounters());
        const auto ev = runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(sender);
        for (const auto& counter : ev->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
            if (counter.GetName() == name) {
                return counter.GetValue();
            }
        }
        UNIT_FAIL("counter " << name << " not found");
        return 0;
    }

    void AddRowsRange(TShardReader & reader, const std::pair<ui64, ui64> rows) {
        const TestTableDescription table;
        reader.SetReplyColumnIds(table.GetColumnIds({ "timestamp" }));
        reader.AddRange(MakeTestRange(rows, true, true, table.Pk));
    }

    Y_UNIT_TEST(ScanAboveLoadedKeysStarts) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        const NOlap::TSnapshot snapshot = StartShardWithRows(runtime, sender);
        SetLoadedKeyRanges(runtime, sender, csControllerGuard, LoadedFromRow(500));

        TShardReader reader(runtime, TTestTxConfig::TxTablet0, TableId, snapshot);
        AddRowsRange(reader, { 600, 700 });
        const auto rows = reader.ReadAll();
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 101);
    }

    Y_UNIT_TEST(ScanBelowLoadedKeysWaitsUntilCovered) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        const NOlap::TSnapshot snapshot = StartShardWithRows(runtime, sender);
        SetLoadedKeyRanges(runtime, sender, csControllerGuard, LoadedFromRow(500));

        TShardReader reader(runtime, TTestTxConfig::TxTablet0, TableId, snapshot);
        AddRowsRange(reader, { 100, 200 });
        reader.StartScanner();
        UNIT_ASSERT(!reader.WaitScannerAnswer(NoAnswerWait));

        SetLoadedKeyRanges(runtime, sender, csControllerGuard, LoadedFromRow(300));
        UNIT_ASSERT(!reader.WaitScannerAnswer(NoAnswerWait));

        SetLoadedKeyRanges(runtime, sender, csControllerGuard, LoadedFromRow(100));
        UNIT_ASSERT(reader.WaitScannerAnswer());
        reader.Ack();
        const auto rows = reader.ContinueReadAll();
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 101);
    }

    Y_UNIT_TEST(ScanWaitingForLoadedPortionsIsCounted) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        const NOlap::TSnapshot snapshot = StartShardWithRows(runtime, sender);
        SetLoadedKeyRanges(runtime, sender, csControllerGuard, LoadedFromRow(500));

        TShardReader reader(runtime, TTestTxConfig::TxTablet0, TableId, snapshot);
        AddRowsRange(reader, { 100, 200 });
        reader.StartScanner();
        UNIT_ASSERT(!reader.WaitScannerAnswer(NoAnswerWait));
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, sender, "ScansWaitingLoadedPortions"), 1);

        SetLoadedKeyRanges(runtime, sender, csControllerGuard, NOlap::TLoadedKeyRanges::BuildEverything());
        UNIT_ASSERT(reader.WaitScannerAnswer());
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, sender, "ScansWaitingLoadedPortions"), 0);
    }

    Y_UNIT_TEST(ScanWaitingForSnapshotIsCounted) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        const NOlap::TSnapshot snapshot = StartShardWithRows(runtime, sender);

        TShardReader reader(runtime, TTestTxConfig::TxTablet0, TableId, NOlap::TSnapshot(snapshot.GetPlanStep() + 1000000, Max<ui64>()));
        AddRowsRange(reader, { 100, 200 });
        reader.StartScanner();
        UNIT_ASSERT(!reader.WaitScannerAnswer(NoAnswerWait));
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, sender, "ScansWaitingSnapshot"), 1);
    }

    // an insert reads the rows it writes through an internal scan
    Y_UNIT_TEST(InsertWaitsBelowLoadedKeys) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        Y_UNUSED(StartShardWithRows(runtime, sender));
        SetLoadedKeyRanges(runtime, sender, csControllerGuard, LoadedFromRow(5000));

        const TestTableDescription table;
        UNIT_ASSERT(WriteData(runtime, sender, 2, TableId, MakeTestBlob({ 2000, 2010 }, table.Schema), table.Schema, false, nullptr,
            NEvWrite::EModificationType::Insert));
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(handle, NoAnswerWait));

        SetLoadedKeyRanges(runtime, sender, csControllerGuard, NOlap::TLoadedKeyRanges::BuildEverything());
        UNIT_ASSERT_VALUES_EQUAL(WaitWriteResult(runtime, TTestTxConfig::TxTablet0), (ui32)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(SysViewScanWaitsUntilEverythingIsLoaded) {
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        const NOlap::TSnapshot snapshot = StartShardWithRows(runtime, sender);
        SetLoadedKeyRanges(runtime, sender, csControllerGuard, LoadedFromRow(0));

        TShardReader reader(runtime, TTestTxConfig::TxTablet0, TableId, snapshot);
        reader.SetTablePath("/.sys/primary_index_portion_stats");
        reader.SetReplyColumnIds({ NKikimr::NSysView::Schema::PrimaryIndexPortionStats::PortionId::ColumnId });
        reader.StartScanner();
        UNIT_ASSERT(!reader.WaitScannerAnswer(NoAnswerWait));

        SetLoadedKeyRanges(runtime, sender, csControllerGuard, NOlap::TLoadedKeyRanges::BuildEverything());
        UNIT_ASSERT(reader.WaitScannerAnswer());
        reader.Ack();
        Y_UNUSED(reader.ContinueReadAll());
        UNIT_ASSERT(reader.IsCorrectlyFinished());
    }
}

}   // namespace NKikimr
