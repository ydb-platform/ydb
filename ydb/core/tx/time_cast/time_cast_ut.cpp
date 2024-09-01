#include "time_cast.h"
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMediatorTimeCastTest {

    using namespace Tests;

    Y_UNIT_TEST_SUITE(MediatorTimeCast) {

        void SimulateSleep(TTestActorRuntime& runtime, TDuration duration) {
            auto sender = runtime.AllocateEdgeActor();
            runtime.Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), duration);
            runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender);
        }

        void SendSubscribeRequest(TTestActorRuntime& runtime, const TActorId& sender, ui64 coordinatorId, ui64 cookie = 0) {
            auto request = MakeHolder<TEvMediatorTimecast::TEvSubscribeReadStep>(coordinatorId);
            runtime.Send(new IEventHandle(MakeMediatorTimecastProxyID(), sender, request.Release(), 0, cookie), 0, true);
        }

        TEvMediatorTimecast::TEvSubscribeReadStepResult::TPtr WaitSubscribeResult(TTestActorRuntime& runtime, const TActorId& sender) {
            return runtime.GrabEdgeEventRethrow<TEvMediatorTimecast::TEvSubscribeReadStepResult>(sender);
        }

        void SendWaitRequest(TTestActorRuntime& runtime, const TActorId& sender, ui64 coordinatorId, ui64 readStep, ui64 cookie = 0) {
            auto request = MakeHolder<TEvMediatorTimecast::TEvWaitReadStep>(coordinatorId, readStep);
            runtime.Send(new IEventHandle(MakeMediatorTimecastProxyID(), sender, request.Release(), 0, cookie), 0, true);
        }

        TEvMediatorTimecast::TEvNotifyReadStep::TPtr WaitNotifyResult(TTestActorRuntime& runtime, const TActorId& sender) {
            return runtime.GrabEdgeEventRethrow<TEvMediatorTimecast::TEvNotifyReadStep>(sender);
        }

        Y_UNIT_TEST(ReadStepSubscribe) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetUseRealThreads(false);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto& runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NActors::NLog::PRI_DEBUG);

            auto sender = runtime.AllocateEdgeActor();
            ui64 coordinatorId = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

            SendSubscribeRequest(runtime, sender, coordinatorId);
            auto result = WaitSubscribeResult(runtime, sender);
            auto stepPtr = result->Get()->ReadStep;

            ui64 readStep1 = stepPtr->Get();
            UNIT_ASSERT_GE(readStep1, result->Get()->LastReadStep);

            SimulateSleep(runtime, TDuration::Seconds(5));

            ui64 readStep2 = stepPtr->Get();
            UNIT_ASSERT_GT(readStep2, readStep1);

            ui64 waitStep = readStep2 + 2000;
            SendWaitRequest(runtime, sender, coordinatorId, waitStep);

            {
                auto notify = WaitNotifyResult(runtime, sender);
                UNIT_ASSERT_GE(notify->Get()->ReadStep, waitStep);
                UNIT_ASSERT_GE(notify->Get()->ReadStep, stepPtr->Get());
            }

            ui64 waitStep2 = stepPtr->Get() + 5000;
            RebootTablet(runtime, coordinatorId, sender);
            SendWaitRequest(runtime, sender, coordinatorId, waitStep2);

            {
                auto notify = WaitNotifyResult(runtime, sender);
                UNIT_ASSERT_GE(notify->Get()->ReadStep, waitStep2);
                UNIT_ASSERT_GE(notify->Get()->ReadStep, stepPtr->Get());
            }
        }

        class TTargetTablet
            : public TActor<TTargetTablet>
            , public NTabletFlatExecutor::TTabletExecutedFlat
        {
        public:
            TTargetTablet(const TActorId& tablet, TTabletStorageInfo* info)
                : TActor(&TThis::StateInit)
                , TTabletExecutedFlat(info, tablet, nullptr)
            {}

        private:
            STFUNC(StateInit) {
                StateInitImpl(ev, SelfId());
            }

            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvTxProcessing::TEvPlanStep, Handle);
                default:
                    HandleDefaultEvents(ev, SelfId());
                }
            }

            void Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev) {
                ui64 step = ev->Get()->Record.GetStep();
                THashMap<TActorId, TVector<ui64>> acks;
                for (const auto& tx : ev->Get()->Record.GetTransactions()) {
                    ui64 txId = tx.GetTxId();
                    TActorId owner = ActorIdFromProto(tx.GetAckTo());
                    acks[owner].push_back(txId);
                }
                for (auto& pr : acks) {
                    auto owner = pr.first;
                    Send(owner, new TEvTxProcessing::TEvPlanStepAck(TabletID(), step, pr.second.begin(), pr.second.end()));
                }
                Send(ev->Sender, new TEvTxProcessing::TEvPlanStepAccepted(TabletID(), step));
            }

        private:
            void DefaultSignalTabletActive(const TActorContext&) override {
                // must be empty
            }

            void OnActivateExecutor(const TActorContext& ctx) override {
                Become(&TThis::StateWork);
                SignalTabletActive(ctx);
            }

            void OnDetach(const TActorContext&) override {
                PassAway();
            }

            void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext&) override {
                PassAway();
            }
        };

        TActorId BootTarget(TTestActorRuntime& runtime, ui64 tabletId) {
            auto boot = CreateTestBootstrapper(runtime,
                CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
                [](const TActorId& tablet, TTabletStorageInfo* info) {
                    return new TTargetTablet(tablet, info);
                });

            {
                TDispatchOptions options;
                options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
                runtime.DispatchEvents(options);
            }

            return boot;
        }

        ui64 PlanTx(const TServer::TPtr& server, ui64 txId, std::vector<ui64> tablets) {
            auto& runtime = *server->GetRuntime();
            ui64 coordinatorId = ChangeStateStorage(TTestTxConfig::Coordinator, server->GetSettings().Domain);
            auto req = std::make_unique<TEvTxProxy::TEvProposeTransaction>(
                coordinatorId, txId, 0, Min<ui64>(), Max<ui64>());
            auto* affectedSet = req->Record.MutableTransaction()->MutableAffectedSet();
            for (ui64 tabletId : tablets) {
                auto* x = affectedSet->Add();
                x->SetTabletId(tabletId);
                x->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);
            }
            auto sender = runtime.AllocateEdgeActor();
            ForwardToTablet(runtime, coordinatorId, sender, req.release());
            for (;;) {
                auto ev = runtime.GrabEdgeEventRethrow<TEvTxProxy::TEvProposeTransactionStatus>(sender);
                auto* msg = ev->Get();
                switch (msg->GetStatus()) {
                    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
                        break;
                    case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
                        return msg->Record.GetStepId();
                    default:
                        UNIT_ASSERT_C(false, "Unexpected status " << int(msg->GetStatus()));
                }
            }
        }

        TMediatorTimecastEntry::TCPtr RegisterTablet(const TServer::TPtr& server, ui64 tabletId) {
            auto& runtime = *server->GetRuntime();
            auto sender = runtime.AllocateEdgeActor();
            auto params = ExtractProcessingParams(*runtime.GetAppData().DomainsInfo->GetDomain());
            auto req = std::make_unique<TEvMediatorTimecast::TEvRegisterTablet>(tabletId, params);
            runtime.Send(new IEventHandle(MakeMediatorTimecastProxyID(), sender, req.release()), 0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TEvMediatorTimecast::TEvRegisterTabletResult>(sender);
            return ev->Get()->Entry;
        }

        Y_UNIT_TEST(GranularTimecast) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetDomainPlanResolution(500)
                .SetDomainTimecastBuckets(1)
                .SetEnableGranularTimecast(true)
                .SetUseRealThreads(false);

            TServer::TPtr server = new TServer(serverSettings);

            auto& runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NActors::NLog::PRI_DEBUG);

            auto sender = runtime.AllocateEdgeActor();

            ui64 tablet1 = ChangeStateStorage(TTestTxConfig::TxTablet0, server->GetSettings().Domain);
            BootTarget(runtime, tablet1);
            ui64 tablet2 = ChangeStateStorage(TTestTxConfig::TxTablet1, server->GetSettings().Domain);
            BootTarget(runtime, tablet2);
            ui64 tablet3 = ChangeStateStorage(TTestTxConfig::TxTablet3, server->GetSettings().Domain);
            BootTarget(runtime, tablet3);

            auto entry1 = RegisterTablet(server, tablet1);
            auto entry2 = RegisterTablet(server, tablet2);
            auto entry3 = RegisterTablet(server, tablet3);

            ui64 testStep1 = entry1->Get();
            runtime.SimulateSleep(TDuration::Seconds(2));
            ui64 testStep2 = entry1->Get();
            Cerr << "... have step " << testStep1 << " and " << testStep2 << " after sleep" << Endl;
            UNIT_ASSERT_C(testStep1 < testStep2, "Have step " << testStep1 << " and " << testStep2 << " after sleep");

            TBlockEvents<TEvTxProcessing::TEvPlanStepAck> blockAcks(runtime);
            TBlockEvents<TEvTxProcessing::TEvPlanStep> blockPlan1(runtime,
                [&](auto& ev) { return ev->Get()->Record.GetTabletID() == tablet1; });
            TBlockEvents<TEvTxProcessing::TEvPlanStep> blockPlan2(runtime,
                [&](auto& ev) { return ev->Get()->Record.GetTabletID() == tablet2; });

            ui64 stepTx1 = PlanTx(server, 1, { tablet1, tablet2 });
            Cerr << "... tx1 planned at step " << stepTx1 << Endl;
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(blockPlan1.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(blockPlan2.size(), 1u);

            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;

            UNIT_ASSERT_C(entry1->Get() == stepTx1-1, "tablet1 step " << entry1->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry2->Get() == stepTx1-1, "tablet2 step " << entry2->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry3->Get() == stepTx1, "tablet3 step " << entry3->Get() << " not at " << stepTx1);

            ui64 stepTx2 = PlanTx(server, 2, { tablet1, tablet2 });
            Cerr << "... tx2 planned at step " << stepTx2 << Endl;
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(blockPlan1.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(blockPlan2.size(), 2u);

            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;

            UNIT_ASSERT_C(entry1->Get() == stepTx1-1, "tablet1 step " << entry1->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry2->Get() == stepTx1-1, "tablet2 step " << entry2->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry3->Get() == stepTx2, "tablet3 step " << entry3->Get() << " not at " << stepTx2);

            Cerr << "... unblocking tx1 at tablet2" << Endl;
            blockPlan2.Unblock(1);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;

            UNIT_ASSERT_C(entry1->Get() == stepTx1-1, "tablet1 step " << entry1->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry2->Get() == stepTx2-1, "tablet2 step " << entry2->Get() << " not at " << (stepTx2-1));
            UNIT_ASSERT_C(entry3->Get() == stepTx2, "tablet3 step " << entry3->Get() << " not at " << stepTx2);

            Cerr << "... unblocking tx2 at tablet2" << Endl;
            blockPlan2.Unblock(1);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;

            UNIT_ASSERT_C(entry1->Get() == stepTx1-1, "tablet1 step " << entry1->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry2->Get() == stepTx2, "tablet2 step " << entry2->Get() << " not at " << stepTx2);
            UNIT_ASSERT_C(entry3->Get() == stepTx2, "tablet3 step " << entry3->Get() << " not at " << stepTx2);

            TBlockEvents<TEvMediatorTimecast::TEvNotifyPlanStep> blockNotify(runtime);

            // This tests notify when latest step is updated for a non-frozen tablet
            runtime.Send(new IEventHandle(MakeMediatorTimecastProxyID(), sender, new TEvMediatorTimecast::TEvWaitPlanStep(tablet2, stepTx2+1)));
            runtime.WaitFor("step notify", [&]{ return blockNotify.size() > 0; });
            UNIT_ASSERT_VALUES_EQUAL(blockNotify.size(), 1u);
            ui64 notifiedStep = blockNotify[0]->Get()->PlanStep;
            blockNotify.clear();
            Cerr << "... notified at step " << notifiedStep << Endl;
            UNIT_ASSERT_C(notifiedStep >= stepTx2+1, "notified at " << notifiedStep << " expected at least " << (stepTx2+1));

            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;
            UNIT_ASSERT_C(entry1->Get() == stepTx1-1, "tablet1 step " << entry1->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry2->Get() == notifiedStep, "tablet2 step " << entry2->Get() << " not at " << notifiedStep);
            UNIT_ASSERT_C(entry3->Get() == notifiedStep, "tablet3 step " << entry3->Get() << " not at " << notifiedStep);

            TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockUpdate(runtime);

            // Plan a new tx and double check nothing has changed yet
            ui64 stepTx3 = PlanTx(server, 3, { tablet2 });
            Cerr << "... tx3 planned at step " << stepTx3 << Endl;
            UNIT_ASSERT_VALUES_EQUAL(entry1->Get(), stepTx1-1);
            UNIT_ASSERT_VALUES_EQUAL(entry2->Get(), notifiedStep);
            UNIT_ASSERT_VALUES_EQUAL(entry3->Get(), notifiedStep);

            // This tests notify after latest step is updated and then tablet's frozen step changes
            runtime.Send(new IEventHandle(MakeMediatorTimecastProxyID(), sender, new TEvMediatorTimecast::TEvWaitPlanStep(tablet2, stepTx3)));
            runtime.WaitFor("granular update", [&]{ return blockUpdate.size() > 0 && blockUpdate.back()->Get()->Record.GetLatestStep() >= stepTx3; });
            UNIT_ASSERT_VALUES_EQUAL(blockUpdate.size(), 1u); // new step + freeze
            blockUpdate.Unblock();
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;
            UNIT_ASSERT_C(entry1->Get() == stepTx1-1, "tablet1 step " << entry1->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry2->Get() == stepTx3-1, "tablet2 step " << entry2->Get() << " not at " << (stepTx3-1));
            UNIT_ASSERT_C(entry3->Get() == stepTx3, "tablet3 step " << entry3->Get() << " not at " << stepTx3);
            UNIT_ASSERT_VALUES_EQUAL(blockNotify.size(), 0u); // not notified yet!

            blockPlan2.Unblock();
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(blockUpdate.size(), 1u); // unfreeze
            blockUpdate.Unblock();
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;
            UNIT_ASSERT_C(entry1->Get() == stepTx1-1, "tablet1 step " << entry1->Get() << " not at " << (stepTx1-1));
            UNIT_ASSERT_C(entry2->Get() == stepTx3, "tablet2 step " << entry2->Get() << " not at " << stepTx3);
            UNIT_ASSERT_C(entry3->Get() == stepTx3, "tablet3 step " << entry3->Get() << " not at " << stepTx3);

            UNIT_ASSERT_VALUES_EQUAL(blockNotify.size(), 1u); // notified!
            Cerr << "... notified at step " << blockNotify[0]->Get()->PlanStep << Endl;
            UNIT_ASSERT_VALUES_EQUAL(blockNotify[0]->Get()->PlanStep, stepTx3);
            blockNotify.clear();

            blockAcks.clear();
            blockPlan1.clear();
            blockPlan2.clear();
            blockUpdate.clear();

            // Test that attaching to a lagging mediator doesn't cause time to go backwards
            Cerr << "... restarting mediator" << Endl;
            ui64 mediatorId = ChangeStateStorage(TTestTxConfig::Mediator, server->GetSettings().Domain);
            RebootTablet(runtime, mediatorId, sender);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(blockPlan1.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(blockPlan2.size(), 3u);
            for (;;) {
                while (blockUpdate.size() > 0) {
                    Cerr << "... unblocking update: " << blockUpdate[0]->Get()->Record.ShortDebugString() << Endl;
                    blockUpdate.Unblock(1);
                    runtime.SimulateSleep(TDuration::MilliSeconds(1));
                    UNIT_ASSERT_VALUES_EQUAL(entry1->Get(), stepTx1-1);
                    UNIT_ASSERT_VALUES_EQUAL(entry2->Get(), stepTx3);
                    UNIT_ASSERT_VALUES_EQUAL(entry3->Get(), stepTx3);
                }
                if (blockPlan2.empty()) {
                    break;
                }
                Cerr << "... unblocking plan for tablet2" << Endl;
                blockPlan2.Unblock(1);
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
            }

            blockAcks.clear();
            blockPlan1.clear();
            blockPlan2.clear();
            blockUpdate.clear();

            // Test that attaching to a mediator without granular support allows time to go forward
            Cerr << "... restarting mediator" << Endl;
            RebootTablet(runtime, mediatorId, sender);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(blockPlan1.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(blockPlan2.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(entry1->Get(), stepTx1-1);
            UNIT_ASSERT_VALUES_EQUAL(entry2->Get(), stepTx3);
            UNIT_ASSERT_VALUES_EQUAL(entry3->Get(), stepTx3);

            Cerr << "... fully unblocking tx1" << Endl;
            blockPlan1.Unblock(1);
            blockPlan2.Unblock(1);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(entry1->Get(), stepTx1);
            UNIT_ASSERT_VALUES_EQUAL(entry2->Get(), stepTx3);
            UNIT_ASSERT_VALUES_EQUAL(entry3->Get(), stepTx3);

            Cerr << "... fully unblocking tx2" << Endl;
            blockPlan1.Unblock(1);
            blockPlan2.Unblock(1);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(entry1->Get(), stepTx2);
            UNIT_ASSERT_VALUES_EQUAL(entry2->Get(), stepTx3);
            UNIT_ASSERT_VALUES_EQUAL(entry3->Get(), stepTx3);

            Cerr << "... fully unblocking tx3" << Endl;
            blockPlan2.Unblock(1);
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            Cerr << "... tablet1 at " << entry1->Get() << Endl;
            Cerr << "... tablet2 at " << entry2->Get() << Endl;
            Cerr << "... tablet3 at " << entry3->Get() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(entry1->Get(), stepTx3);
            UNIT_ASSERT_VALUES_EQUAL(entry2->Get(), stepTx3);
            UNIT_ASSERT_VALUES_EQUAL(entry3->Get(), stepTx3);
        }

    } // Y_UNIT_TEST_SUITE(MediatorTimeCast)

} // namespace NKikimr::NMediatorTimeCastTest
