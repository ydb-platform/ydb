#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(TColumnShardIndexPortionsByLastKey) {
    const ui64 TableId = 1;

    Y_UNIT_TEST(MatchesPortionsThroughCompactionAndCleanup) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        runtime.DispatchEvents(options);

        const TestTableDescription table;
        Y_UNUSED(SetupSchema(runtime, sender, TableId, table));
        const TString data = MakeTestBlob({ 0, 1000 }, table.Schema);
        ui64 writeId = 0;
        ui64 txId = 100;
        const auto writeAndCommit = [&]() {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, TableId, data, table.Schema, true, &writeIds));
            PlanCommit(runtime, sender, ProposeCommit(runtime, sender, txId, writeIds), txId);
            ++txId;
        };

        for (ui32 i = 0; i < 3; ++i) {
            writeAndCommit();
        }
        UNIT_ASSERT_VALUES_EQUAL(AssertIndexMatchesPortions(runtime, TTestTxConfig::TxTablet0, table.Pk), 3);

        controller->EnableBackground(NYDBTest::ICSController::EBackground::Compaction);
        for (ui32 i = 0; i < 60 && !controller->GetCompactionFinishedCounter().Val(); ++i) {
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvPrivate::TEvPeriodicWakeup(true));
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_GT(controller->GetCompactionFinishedCounter().Val(), 0);
        // the compacted portions are added, the old ones stay until cleanup
        UNIT_ASSERT_GT(AssertIndexMatchesPortions(runtime, TTestTxConfig::TxTablet0, table.Pk), 3);

        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        controller->SetOverrideStalenessLivetimePing(TDuration::Zero());
        controller->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        // cleanup waits for the plan step to pass the read window, and commits move it
        for (ui32 i = 0; i < 60 && !controller->GetPortionsErasedCounter().Val(); ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            writeAndCommit();
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvPrivate::TEvPeriodicWakeup(true));
        }
        UNIT_ASSERT_GT(controller->GetPortionsErasedCounter().Val(), 0);
        AssertIndexMatchesPortions(runtime, TTestTxConfig::TxTablet0, table.Pk);
    }
}

}   // namespace NKikimr
