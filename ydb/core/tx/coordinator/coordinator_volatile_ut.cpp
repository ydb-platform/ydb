#include <ydb/core/tx/coordinator/public/events.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NFlatTxCoordinator::NTest {

    using namespace Tests;

    class TPlanTargetTablet
        : public TActor<TPlanTargetTablet>
        , public NTabletFlatExecutor::TTabletExecutedFlat
    {
    public:
        TPlanTargetTablet(const TActorId& tablet, TTabletStorageInfo* info)
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

    Y_UNIT_TEST_SUITE(CoordinatorVolatile) {

        Y_UNIT_TEST(PlanResentOnReboots) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(1)
                .SetUseRealThreads(false);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto &runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);

            auto sender = runtime.AllocateEdgeActor();
            ui64 coordinatorId = ChangeStateStorage(Coordinator, server->GetSettings().Domain);
            ui64 mediatorId = ChangeStateStorage(Mediator, server->GetSettings().Domain);
            ui64 tabletId = ChangeStateStorage(TTestTxConfig::TxTablet0, server->GetSettings().Domain);

            CreateTestBootstrapper(runtime,
                CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
                [](const TActorId& tablet, TTabletStorageInfo* info) {
                    return new TPlanTargetTablet(tablet, info);
                });

            {
                TDispatchOptions options;
                options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
                runtime.DispatchEvents(options);
            }

            bool dropAcks = true;
            bool dropStateRequests = false;
            std::vector<ui64> observedSteps;
            std::vector<std::unique_ptr<IEventHandle>> acks;
            std::vector<std::unique_ptr<IEventHandle>> stateRequests;
            auto observer = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvTxProcessing::TEvPlanStep::EventType: {
                        auto* msg = ev->Get<TEvTxProcessing::TEvPlanStep>();
                        observedSteps.push_back(msg->Record.GetStep());
                        break;
                    }
                    case TEvTxProcessing::TEvPlanStepAck::EventType:
                    case TEvTxProcessing::TEvPlanStepAccepted::EventType:
                        if (dropAcks) {
                            acks.emplace_back(ev.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    case TEvTxCoordinator::TEvCoordinatorStateRequest::EventType:
                        if (dropStateRequests) {
                            stateRequests.emplace_back(ev.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto prevObserverFunc = runtime.SetObserverFunc(observer);

            auto waitFor = [&](const auto& condition, const TString& description) {
                for (int i = 0; i < 5 && !condition(); ++i) {
                    Cerr << "... waiting for " << description << Endl;
                    TDispatchOptions options;
                    options.CustomFinalCondition = [&]() {
                        return condition();
                    };
                    runtime.DispatchEvents(options);
                }
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            };

            ui64 txId = 12345678;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, txId, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            waitFor([&]{ return observedSteps.size() > 0; }, "plan step at tablet");
            ui64 step1 = observedSteps.at(0);
            observedSteps.clear();

            Cerr << "... rebooting tablet" << Endl;
            RebootTablet(runtime, tabletId, sender);

            waitFor([&]{ return observedSteps.size() > 0; }, "plan step resent on tablet reboot");
            UNIT_ASSERT_VALUES_EQUAL(observedSteps.at(0), step1);
            observedSteps.clear();

            Cerr << "... rebooting mediator" << Endl;
            RebootTablet(runtime, mediatorId, sender);

            waitFor([&]{ return observedSteps.size() > 0; }, "plan step resent on mediator reboot");
            UNIT_ASSERT_VALUES_EQUAL(observedSteps.at(0), step1);
            observedSteps.clear();

            Cerr << "... rebooting coordinator+mediator" << Endl;
            RebootTablet(runtime, coordinatorId, sender);
            RebootTablet(runtime, mediatorId, sender);

            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_C(!observedSteps.empty(), "expected plan step resent after coordinator+mediator reboot");
            UNIT_ASSERT_VALUES_EQUAL(observedSteps.at(0), step1);
            observedSteps.clear();

            ui64 txId2 = txId + 1;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, txId2, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            waitFor([&]{ return observedSteps.size() > 0; }, "plan step at tablet");
            ui64 step2 = observedSteps.at(0);
            UNIT_ASSERT_C(step2 > step1 && step2 - step1 < 10,
                "unexpected step " << step2 << " after step " << step1);
            observedSteps.clear();

            dropStateRequests = true;
            Cerr << "... rebooting coordinator+mediator with blocked state" << Endl;
            RebootTablet(runtime, coordinatorId, sender);
            RebootTablet(runtime, mediatorId, sender);

            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(stateRequests.size() > 0);
            UNIT_ASSERT_C(observedSteps.empty(), "unexpected plan step resent after coordinator+mediator reboot");

            acks.clear();
            stateRequests.clear();

            ui64 txId3 = txId2 + 1;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, txId3, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            waitFor([&]{ return observedSteps.size() > 0; }, "plan step at tablet");
            ui64 step3 = observedSteps.at(0);
            UNIT_ASSERT_C(step3 > step2 && step3 - step2 > 100,
                "unexpected step " << step3 << " after step " << step2);
            observedSteps.clear();

            dropAcks = false;
            dropStateRequests = false;
            for (auto& ev : acks) {
                runtime.Send(ev.release(), 0, true);
            }
            acks.clear();

            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            Cerr << "... rebooting coordinator+mediator" << Endl;
            RebootTablet(runtime, coordinatorId, sender);
            RebootTablet(runtime, mediatorId, sender);

            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_C(observedSteps.empty(), "unexpected resend of acknowledged step after coordinator+mediator reboot");

            ui64 txId4 = txId2 + 1;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, txId4, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            waitFor([&]{ return observedSteps.size() > 0; }, "plan step at tablet");
            ui64 step4 = observedSteps.at(0);
            UNIT_ASSERT_C(step4 > step3 && step4 - step3 < 10,
                "unexpected step " << step4 << " after step " << step3);
            observedSteps.clear();
        }

    } // Y_UNIT_TEST_SUITE(CoordinatorVolatile)

} // namespace NKikimr::NFlatTxCoordinator::NTest
