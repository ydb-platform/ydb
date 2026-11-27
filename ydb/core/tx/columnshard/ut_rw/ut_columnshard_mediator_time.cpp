#include <ydb/core/base/tx_processing.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/mediator_time/mediator_time.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace NTxUT;

Y_UNIT_TEST_SUITE(TColumnShardMediatorTime) {
    const ui64 TableId = 1;

    void StartShardWithProcessingParams(TTestBasicRuntime & runtime, TActorId & sender) {
        TTester::Setup(runtime);
        sender = runtime.AllocateEdgeActor();
        const TestTableDescription table;
        Y_UNUSED(PrepareTablet(runtime, TableId, table.Schema, table.Pk.size(), table.Standalone));
    }

    Y_UNIT_TEST(FirstRegistrationBecomesReadyOnce) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        const auto sender = runtime.AllocateEdgeActor();
        TMediatorTime time(TTestTxConfig::TxTablet0, {});
        UNIT_ASSERT(!time.Ready());
        UNIT_ASSERT(!runtime.RunCall([&] {
            const auto ctx = TActivationContext::ActorContextFor(sender);
            time.Register({}, ctx);
            return time.TryBecomeReady(ctx);
        }));
        auto reply = runtime.GrabEdgeEvent<TEvMediatorTimecast::TEvRegisterTabletResult>(sender);
        UNIT_ASSERT(!runtime.RunCall([&] {
            const auto ctx = TActivationContext::ActorContextFor(sender);
            time.OnRegistered(reply->Get()->Entry);
            UNIT_ASSERT(time.TryBecomeReady(ctx));
            UNIT_ASSERT(time.Ready());
            return time.TryBecomeReady(ctx);
        }));
    }

    Y_UNIT_TEST(WriteWaitsForCoordinatorStepAfterRestart) {
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        StartShardWithProcessingParams(runtime, sender);

        const TestTableDescription table;
        const ui64 txId = 11;
        const auto targetStep = ProposeSchemaTx(
            runtime, sender, TTestSchema::CreateTableTxBody(TableId + 1, table.Standalone, table.Schema, table.Pk, {}, /*generation=*/1), txId)
                                    .Val();

        const auto proxy = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeMediatorTimecastProxyID(), proxy);
        const ui64 tabletId = TTestTxConfig::TxTablet0;
        RebootTablet(runtime, tabletId, sender);
        auto registration = runtime.GrabEdgeEvent<TEvMediatorTimecast::TEvRegisterTablet>(proxy);
        const auto shard = registration->Sender;
        auto mediatorStep = MakeIntrusive<TMediatorTimecastSharedEntry>();
        runtime.Send(
            shard, proxy, new TEvMediatorTimecast::TEvRegisterTabletResult(tabletId, new TMediatorTimecastEntry(mediatorStep, mediatorStep)));

        auto subscription = runtime.GrabEdgeEvent<TEvMediatorTimecast::TEvSubscribeReadStep>(proxy);
        runtime.Send(shard, proxy, new TEvMediatorTimecast::TEvSubscribeReadStepResult(subscription->Get()->CoordinatorId, 0, targetStep,
                                       new TMediatorTimecastReadStep(targetStep)));
        auto wait = runtime.GrabEdgeEvent<TEvMediatorTimecast::TEvWaitPlanStep>(proxy, TDuration::Seconds(10));
        UNIT_ASSERT(wait);
        UNIT_ASSERT_VALUES_EQUAL(wait->Get()->PlanStep, targetStep);

        WriteData(runtime, sender, 1, TableId, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, false, nullptr,
            NEvWrite::EModificationType::Upsert, /*lockId=*/0);

        const auto mediator = runtime.AllocateEdgeActor();
        auto plan = MakeHolder<TEvTxProcessing::TEvPlanStep>(targetStep, 0, tabletId);
        plan->Record.AddTransactions()->SetTxId(txId);
        runtime.Send(shard, mediator, plan.Release());
        auto accepted = runtime.GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAccepted>(mediator, TDuration::Seconds(10));
        UNIT_ASSERT(accepted);
        UNIT_ASSERT_VALUES_EQUAL(accepted->Get()->Record.GetStep(), targetStep);
        UNIT_ASSERT_VALUES_EQUAL(accepted->Get()->Record.GetTabletId(), tabletId);
        UNIT_ASSERT(!runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender, TDuration::Seconds(1)));

        mediatorStep->Set(targetStep);
        runtime.Send(shard, proxy, new TEvMediatorTimecast::TEvNotifyPlanStep(tabletId, targetStep));
        auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender, TDuration::Seconds(10));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    Y_UNIT_TEST(LocalSchemeRequestWaitsForFirstRegistration) {
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvMediatorTimecast::TEvRegisterTabletResult> registration(runtime, [](const auto& ev) {
            return ev->Get()->TabletId == TTestTxConfig::TxTablet0;
        });
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);
        runtime.WaitFor("active shard", [&] {
            return csControllerGuard->IsActiveTablet(TTestTxConfig::TxTablet0);
        });

        const TestTableDescription table;
        const ui64 txId = 10;
        const auto planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CreateStandaloneTableTxBody(TableId, table.Schema, table.Pk), txId);
        PlanSchemaTxStepOnly(runtime, sender, NOlap::TSnapshot(planStep, txId));
        runtime.WaitFor("shard registration reply", [&] {
            return !registration.empty();
        });

        auto request = MakeHolder<TEvTablet::TEvLocalSchemeTx>();
        request->Record.SetDryRun(true);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, request.Release());
        UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvTablet::TEvLocalSchemeTxResponse>(sender, TDuration::Seconds(1)));

        UNIT_ASSERT_GT(GetSimpleCounter(runtime, sender, "ColumnShard/EventsWaitingMediatorTime"), 0);

        registration.Unblock();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvTablet::TEvLocalSchemeTxResponse>(sender, TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(GetSimpleCounter(runtime, sender, "ColumnShard/EventsWaitingMediatorTime"), 0);
    }

    Y_UNIT_TEST(ProposalWithProcessingParamsWaitsAfterRestart) {
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        StartShardWithProcessingParams(runtime, sender);
        TBlockEvents<TEvMediatorTimecast::TEvSubscribeReadStepResult> readSteps(runtime);
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        runtime.WaitFor("coordinator reply", [&] {
            return !readSteps.empty();
        });

        const TestTableDescription table;
        const ui64 txId = 11;
        auto proposal = MakeHolder<TEvColumnShard::TEvProposeTransaction>(NKikimrTxColumnShard::TX_KIND_SCHEMA, sender, txId,
            TTestSchema::CreateTableTxBody(TableId + 1, table.Standalone, table.Schema, table.Pk, {}, /*generation=*/1));
        *proposal->Record.MutableProcessingParams() = ExtractProcessingParams(*runtime.GetAppData().DomainsInfo->GetDomain());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, proposal.Release());
        UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender, TDuration::Seconds(1)));

        readSteps.Unblock();
        auto result = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender, TDuration::Seconds(10));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), txId);
        UNIT_ASSERT_EQUAL(result->Get()->Record.GetStatus(), NKikimrTxColumnShard::PREPARED);
    }

    Y_UNIT_TEST(SubDomainUnavailableStopsShardDuringCatchUp) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestBasicRuntime runtime;
        TActorId sender;
        StartShardWithProcessingParams(runtime, sender);

        const ui64 tabletId = TTestTxConfig::TxTablet0;
        TBlockEvents<TEvMediatorTimecast::TEvSubscribeReadStepResult> readSteps(runtime);
        RebootTablet(runtime, tabletId, sender);
        runtime.WaitFor("shard in StateWork awaiting coordinator reply", [&] {
            return controller->IsActiveTablet(tabletId) && !readSteps.empty();
        }, TDuration::Seconds(10));

        const TActorId shard = readSteps.front()->GetRecipientRewrite();
        runtime.Send(shard, sender, new TEvTxProxySchemeCache::TEvWatchNotifyUnavailable(0, "/Root", TPathId(1, 1)));
        runtime.WaitFor("shard stopped while coordinator reply is blocked", [&] {
            return !runtime.FindActor(shard);
        }, TDuration::Seconds(10));
    }

    Y_UNIT_TEST(CatchUpUnsubscribesFromAllCoordinatorsWhenReady) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        const auto sender = runtime.AllocateEdgeActor();
        TMediatorTime time(TTestTxConfig::TxTablet0, {});
        NKikimrSubDomains::TProcessingParams params;
        params.AddCoordinators(1);
        params.AddCoordinators(2);
        UNIT_ASSERT(!runtime.RunCall([&] {
            const auto ctx = TActivationContext::ActorContextFor(sender);
            time.RegisterAndStartCatchUp(params, ctx);
            return time.TryBecomeReady(ctx);
        }));

        auto registration = runtime.GrabEdgeEvent<TEvMediatorTimecast::TEvRegisterTabletResult>(sender);
        auto firstReply = runtime.GrabEdgeEvent<TEvMediatorTimecast::TEvSubscribeReadStepResult>(sender);
        TBlockEvents<TEvMediatorTimecast::TEvUnsubscribeReadStep> unsubscribeAll(runtime, [&](const auto& ev) {
            return ev->Sender == sender && ev->Get()->CoordinatorId == 0;
        });
        UNIT_ASSERT(runtime.RunCall([&] {
            const auto ctx = TActivationContext::ActorContextFor(sender);
            time.OnRegistered(registration->Get()->Entry);
            time.OnNextReadStep(firstReply->Get()->CoordinatorId, firstReply->Get()->NextReadStep, ctx);
            return time.TryBecomeReady(ctx);
        }));
        runtime.WaitFor("unsubscribe from all coordinators", [&] {
            return !unsubscribeAll.empty();
        }, TDuration::Seconds(1));
    }
}

}   // namespace NKikimr
