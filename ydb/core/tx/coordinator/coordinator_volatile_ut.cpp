#include <ydb/core/tx/coordinator/public/events.h>
#include <ydb/core/tx/coordinator/coordinator_impl.h>
#include <ydb/core/tx/coordinator/coordinator_hooks.h>
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
            auto observer = [&](TAutoPtr<IEventHandle>& ev) {
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

        Y_UNIT_TEST(MediatorReconnectPlanRace) {
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

            TActorId mediatorQueue;
            std::vector<std::unique_ptr<IEventHandle>> mediatorQueueSteps;
            auto blockMediatorQueueSteps = runtime.AddObserver<TEvMediatorQueueStep>([&](TEvMediatorQueueStep::TPtr& ev) {
                mediatorQueue = ev->GetRecipientRewrite();
                mediatorQueueSteps.emplace_back(ev.Release());
                Cerr << "... blocked TEvMediatorQueueStep for " << mediatorQueue << Endl;
            });

            std::vector<ui64> observedSteps;
            auto stepsObserver = runtime.AddObserver<TEvTxProcessing::TEvPlanStep>([&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
                auto* msg = ev->Get();
                observedSteps.push_back(msg->Record.GetStep());
            });

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
                // Not necessary, but we test volatile transactions here
                tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            waitFor([&]{ return mediatorQueueSteps.size() > 0; }, "TEvMediatorQueueStep");
            UNIT_ASSERT_VALUES_EQUAL(mediatorQueueSteps.size(), 1u);

            // We shouldn't see any steps yet
            UNIT_ASSERT_VALUES_EQUAL(observedSteps.size(), 0u);

            auto injectMediatorQueueStep = runtime.AddObserver<TEvTabletPipe::TEvClientDestroyed>([&](TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
                if (ev->GetRecipientRewrite() == mediatorQueue) {
                    Cerr << "... found pipe disconnect at " << mediatorQueue << Endl;
                    // Stop blocking mediator queue steps
                    // This seems to be safe, since we remove someone else from std::list
                    blockMediatorQueueSteps.Remove();
                    // Inject blocked mediator steps into queue mailbox, they will be handled after the disconnect
                    for (auto& ev : mediatorQueueSteps) {
                        runtime.Send(ev.release(), 0, true);
                    }
                    mediatorQueueSteps.clear();
                }
            });

            Cerr << "... rebooting mediator" << Endl;
            RebootTablet(runtime, mediatorId, sender);

            waitFor([&]{ return mediatorQueueSteps.empty(); }, "injected mediator steps");

            // We must observe the plan step soon
            runtime.SimulateSleep(TDuration::Seconds(2));
            UNIT_ASSERT_VALUES_EQUAL(observedSteps.size(), 1u);
        }

        /**
         * Tests a scenario where coordinator's volatile lease expires, which
         * causes coordinator to update the lease during volatile planning.
         * That transaction is migrated to a newer instance, but commit updating
         * last known step fails. A bug caused new instances to reach a confused
         * state, which could cause it to attempt planning more transactions in
         * the same step, violating invariants.
         */
        Y_UNIT_TEST(CoordinatorMigrateUncommittedVolatileTx) {
            struct TCoordinatorHooks : public ICoordinatorHooks {
                std::vector<ui64> PlannedSteps;

                void BeginPlanStep(ui64 tabletId, ui64 generation, ui64 planStep) override {
                    Cerr << "... coordinator " << tabletId << " gen " << generation << " is planning step " << planStep << Endl;
                    PlannedSteps.push_back(planStep);
                }
            } hooks;
            TCoordinatorHooksGuard hooksGuard(hooks);

            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainPlanResolution(50);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto &runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);
            // runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR, NActors::NLog::PRI_DEBUG);
            // runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, NActors::NLog::PRI_DEBUG);
            // runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TABLETQUEUE, NActors::NLog::PRI_DEBUG);

            auto sender = runtime.AllocateEdgeActor();
            ui64 coordinatorId = ChangeStateStorage(Coordinator, server->GetSettings().Domain);
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

            // Wait for the first idle mediator step
            waitFor([&]{ return hooks.PlannedSteps.size() >= 2; }, "the first two planned steps");

            auto oldTimestamp = runtime.GetCurrentTime();
            auto oldCoordinatorSysActorId = ResolveTablet(runtime, coordinatorId, 0, /* sysTablet */ true);
            auto oldCoordinatorUserActorId = ResolveTablet(runtime, coordinatorId, 0, /* sysTablet */ false);

            // Sleep for 500ms, so the default volatile lease of 250ms will expire
            runtime.SimulateSleep(TDuration::MilliSeconds(500));

            hooks.PlannedSteps.clear();

            // Start blocking EvPut responses for the new plan (but allowing data to commit)
            std::vector<std::unique_ptr<IEventHandle>> blockedPutResponses;
            auto blockPutResponses = runtime.AddObserver<TEvBlobStorage::TEvPutResult>(
                [&](TEvBlobStorage::TEvPutResult::TPtr& ev) {
                    auto* msg = ev->Get();
                    if (hooks.PlannedSteps.size() > 0 && msg->Id.TabletID() == coordinatorId) {
                        // Block commits from coordinator
                        Cerr << "... blocking put " << msg->Id << " response" << Endl;
                        blockedPutResponses.emplace_back(ev.Release());
                    }
                });

            // Block target tablet's accept messages to keep transactions in mediator
            std::vector<std::unique_ptr<IEventHandle>> blockedPlanStepAccepted;
            auto blockPlanStepAccepted = runtime.AddObserver<TEvTxProcessing::TEvPlanStepAccepted>(
                [&](TEvTxProcessing::TEvPlanStepAccepted::TPtr& ev) {
                    auto* msg = ev->Get();
                    if (msg->Record.GetTabletId() == tabletId) {
                        Cerr << "... blocked accept from " << tabletId << Endl;
                        blockedPlanStepAccepted.emplace_back(ev.Release());
                    }
                });

            // Plan a persistent transaction
            ui64 persistentTxId = 10000000;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, persistentTxId, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            // Wait until we have some commit responses blocked
            // This will ensure planned tx is persisted, but coordinator will not act on it yet
            waitFor([&]{ return blockedPutResponses.size() > 0; }, "blocked put responses");

            UNIT_ASSERT_VALUES_EQUAL(hooks.PlannedSteps.size(), 1u);
            ui64 persistentPlanStep = hooks.PlannedSteps.at(0);
            hooks.PlannedSteps.clear();

            // Stop blocking put responses
            blockPutResponses.Remove();

            // Start blocking EvPut requests (not allowing data to commit)
            std::vector<std::unique_ptr<IEventHandle>> blockedPutRequests;
            auto blockPutRequests = runtime.AddObserver<TEvBlobStorage::TEvPut>(
                [&](TEvBlobStorage::TEvPut::TPtr& ev) {
                    auto* msg = ev->Get();
                    if (msg->Id.TabletID() == coordinatorId) {
                        // Block commits from coordinator
                        Cerr << "... blocking put " << msg->Id << " request" << Endl;
                        blockedPutRequests.emplace_back(ev.Release());
                    }
                });

            // Plan a volatile transaction, expected to be planned for Step+1
            ui64 volatileTxId1 = 10000010;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, volatileTxId1, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            // Wait until it's actually planned
            waitFor([&]{ return hooks.PlannedSteps.size() >= 1; }, "planned volatile tx");

            UNIT_ASSERT_VALUES_EQUAL(hooks.PlannedSteps.size(), 1u);
            ui64 volatilePlanStep = hooks.PlannedSteps.at(0);
            hooks.PlannedSteps.clear();

            UNIT_ASSERT_C(volatilePlanStep > persistentPlanStep,
                "Volatile plan step " << volatilePlanStep << " should be after persistent plan step " << persistentPlanStep);

            // Make sure everything settles
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            // We expect there to be a commit attempt (extending the lease and updating last planned)
            UNIT_ASSERT_C(blockedPutRequests.size() > 0, "expected to have put requests blocked by now");
            blockPutRequests.Remove();

            // We want to start a new instance in parallel
            // Block the old bootstrapper from starting unwanted instances when current tablet dies
            bool oldTabletStopped = false;
            auto blockOldTabletDead = runtime.AddObserver<TEvTablet::TEvTabletDead>(
                [&](TEvTablet::TEvTabletDead::TPtr& ev) {
                    if (ev->Sender == oldCoordinatorSysActorId) {
                        if (ev->GetRecipientRewrite() == oldCoordinatorUserActorId) {
                            oldTabletStopped = true;
                        } else {
                            ev.Reset();
                        }
                    }
                });

            // New instance will migrate the in-memory state, block it from reaching the new instance temporarily
            std::vector<std::unique_ptr<IEventHandle>> blockedStateResponses;
            auto blockStateResponses = runtime.AddObserver<TEvTxCoordinator::TEvCoordinatorStateResponse>(
                [&](TEvTxCoordinator::TEvCoordinatorStateResponse::TPtr& ev) {
                    Cerr << "... blocking state response from " << ev->Sender << " to " << ev->GetRecipientRewrite() << Endl;
                    Cerr << ev->Get()->Record.DebugString();
                    blockedStateResponses.emplace_back(ev.Release());
                });

            // Rewind to some older time
            runtime.UpdateCurrentTime(oldTimestamp, /* rewind */ true);

            // Start a new bootstrapper, which will boot a new instance in parallel
            Cerr << "... starting a new coordinator instance" << Endl;
            CreateTestBootstrapper(runtime, CreateTestTabletInfo(coordinatorId, TTabletTypes::Coordinator), &CreateFlatTxCoordinator);

            // Wait until new coordinator almost receives the in-memory state
            waitFor([&]{ return blockedStateResponses.size() >= 1; }, "migrated state");

            // Unblock previously blocked blobstorage messages
            // Since new coordinator has started the storage is already blocked
            Cerr << "... unblocking put responses and requests" << Endl;
            for (auto& ev : blockedPutResponses) {
                runtime.Send(ev.release(), 0, true);
            }
            blockedPutResponses.clear();
            for (auto& ev : blockedPutRequests) {
                runtime.Send(ev.release(), 0, true);
            }
            blockedPutRequests.clear();

            // Sleep a little, so everything settles (e.g. committed plan is sent to mediator)
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            waitFor([&]{ return oldTabletStopped; }, "old tablet stopped");
            hooks.PlannedSteps.clear();

            // Unblock the in-memory state transfer
            blockStateResponses.Remove();
            for (auto& ev : blockedStateResponses) {
                runtime.Send(ev.release(), 0, true);
            }
            blockedStateResponses.clear();

            // Make sure new requests go to the new instance
            InvalidateTabletResolverCache(runtime, coordinatorId);

            // Plan another volatile transaction, with a smaller TxId
            ui64 volatileTxId2 = 10000005;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, volatileTxId2, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                tx->SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            // Wait until it's actually planned
            waitFor([&]{ return hooks.PlannedSteps.size() >= 1; }, "planned volatile tx");

            UNIT_ASSERT_VALUES_EQUAL(hooks.PlannedSteps.size(), 1u);
            ui64 volatilePlanStep2 = hooks.PlannedSteps.at(0);
            hooks.PlannedSteps.clear();

            // Wait until everything settles (e.g. mediators receive all pending transactions)
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            // Reboot the target tablet to trigger the original assertion
            Cerr << "... rebooting target tablet" << Endl;
            RebootTablet(runtime, tabletId, sender);

            // Wait until everything settles
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            // Validate the new plan does not go back in time
            UNIT_ASSERT_C(volatilePlanStep2 > volatilePlanStep,
                "New volatile plan step " << volatilePlanStep2 << " is expected to be after " << volatilePlanStep);
        }

        /**
         * This scenario tests an empty volatile plan that is scheduled behind
         * a persistent plan, which finishes committing after an in-memory
         * state has been snapshotted and migrated. There was a bug where this
         * empty plan step would not be considered as confirmed, and could be
         * erroneously considered as unused by a previous generation.
         */
        Y_UNIT_TEST(CoordinatorRestartWithEnqueuedVolatileStep) {
            struct TCoordinatorHooks : public ICoordinatorHooks {
                std::vector<ui64> PlannedSteps;

                void BeginPlanStep(ui64 tabletId, ui64 generation, ui64 planStep) override {
                    Cerr << "... coordinator " << tabletId << " gen " << generation << " is planning step " << planStep << Endl;
                    PlannedSteps.push_back(planStep);
                }
            } hooks;
            TCoordinatorHooksGuard hooksGuard(hooks);

            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(1)
                .SetUseRealThreads(false)
                .SetDomainPlanResolution(50);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto &runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR, NActors::NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, NActors::NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TABLETQUEUE, NActors::NLog::PRI_DEBUG);

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

            // Wait for the first idle mediator step
            waitFor([&]{ return hooks.PlannedSteps.size() >= 2; }, "the first two planned steps");
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            hooks.PlannedSteps.clear();

            auto oldTimestamp = runtime.GetCurrentTime();
            auto oldCoordinatorSysActorId = ResolveTablet(runtime, coordinatorId, 0, /* sysTablet */ true);
            auto oldCoordinatorUserActorId = ResolveTablet(runtime, coordinatorId, 0, /* sysTablet */ false);

            // Start blocking EvPut responses for the new plan (but allowing data to commit)
            std::vector<std::unique_ptr<IEventHandle>> blockedPutResponses;
            auto blockPutResponses = runtime.AddObserver<TEvBlobStorage::TEvPutResult>(
                [&](TEvBlobStorage::TEvPutResult::TPtr& ev) {
                    auto* msg = ev->Get();
                    if (hooks.PlannedSteps.size() > 0 && msg->Id.TabletID() == coordinatorId) {
                        // Block commits from coordinator
                        Cerr << "... blocking put " << msg->Id << " response" << Endl;
                        blockedPutResponses.emplace_back(ev.Release());
                    }
                });

            // Block target tablet's accept messages to keep transactions in mediator
            std::vector<std::unique_ptr<IEventHandle>> blockedPlanStepAccepted;
            auto blockPlanStepAccepted = runtime.AddObserver<TEvTxProcessing::TEvPlanStepAccepted>(
                [&](TEvTxProcessing::TEvPlanStepAccepted::TPtr& ev) {
                    auto* msg = ev->Get();
                    if (msg->Record.GetTabletId() == tabletId) {
                        Cerr << "... blocked accept from " << tabletId << Endl;
                        blockedPlanStepAccepted.emplace_back(ev.Release());
                    }
                });

            // step -> list of transactions
            std::map<ui64, std::vector<ui64>> observedSteps;
            auto observeSteps = runtime.AddObserver<TEvTxCoordinator::TEvCoordinatorStep>(
                [&](TEvTxCoordinator::TEvCoordinatorStep::TPtr& ev) {
                    auto* msg = ev->Get();
                    Cerr << "... observed step:" << Endl;
                    Cerr << msg->Record.DebugString();
                    if (msg->Record.GetCoordinatorID() != coordinatorId) {
                        return;
                    }
                    ui64 step = msg->Record.GetStep();
                    std::vector<ui64> txIds;
                    for (const auto& tx : msg->Record.GetTransactions()) {
                        txIds.push_back(tx.GetTxId());
                    }
                    std::sort(txIds.begin(), txIds.end());
                    auto it = observedSteps.find(step);
                    if (it == observedSteps.end()) {
                        observedSteps[step] = std::move(txIds);
                    } else {
                        auto dumpTxIds = [](const std::vector<ui64>& txIds) -> TString {
                            TStringBuilder sb;
                            sb << "{";
                            bool first = true;
                            for (ui64 txId : txIds) {
                                if (first) {
                                    first = false;
                                } else {
                                    sb << ", ";
                                }
                                sb << txId;
                            }
                            sb << "}";
                            return std::move(sb);
                        };
                        UNIT_ASSERT_C(it->second == txIds,
                            "Step " << step << " changed transactions list "
                            << dumpTxIds(it->second) << " -> " << dumpTxIds(txIds));
                    }
                });

            // txId -> step
            std::map<ui64, ui64> observedTabletTxs;
            auto observeTabletTxs = runtime.AddObserver<TEvTxProcessing::TEvPlanStep>(
                [&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
                    auto* msg = ev->Get();
                    Cerr << "... observed tablet step:" << Endl;
                    Cerr << msg->Record.DebugString();
                    ui64 step = msg->Record.GetStep();
                    for (auto& tx : msg->Record.GetTransactions()) {
                        observedTabletTxs[tx.GetTxId()] = step;
                    }
                });

            // Plan a persistent transaction
            ui64 persistentTxId = 10000000;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, persistentTxId, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            // Wait until we have some commit responses blocked
            waitFor([&]{ return blockedPutResponses.size() > 0; }, "blocked put responses");

            UNIT_ASSERT_VALUES_EQUAL(hooks.PlannedSteps.size(), 1u);
            ui64 persistentPlanStep = hooks.PlannedSteps.at(0);
            hooks.PlannedSteps.clear();

            // Require an empty step, it should be divisible by plan resolution
            runtime.SendToPipe(coordinatorId, sender, new TEvTxProxy::TEvRequirePlanSteps(coordinatorId, persistentPlanStep + 50));

            // Wait until it is also planned
            waitFor([&]{ return hooks.PlannedSteps.size() >= 1; }, "planning for the required step");

            UNIT_ASSERT_VALUES_EQUAL(hooks.PlannedSteps.size(), 1u);
            ui64 volatileEmptyPlanStep = hooks.PlannedSteps.at(0);
            hooks.PlannedSteps.clear();

            // Stop blocking newer put responses
            blockPutResponses.Remove();

            // Make sure everything settles
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            // We want to start a new instance in parallel
            // Block the old bootstrapper from starting unwanted instances when current tablet dies
            bool oldTabletStopped = false;
            auto blockOldTabletDead = runtime.AddObserver<TEvTablet::TEvTabletDead>(
                [&](TEvTablet::TEvTabletDead::TPtr& ev) {
                    if (ev->Sender == oldCoordinatorSysActorId) {
                        if (ev->GetRecipientRewrite() == oldCoordinatorUserActorId) {
                            oldTabletStopped = true;
                        } else {
                            ev.Reset();
                        }
                    }
                });

            // New instance will migrate the in-memory state, block it from reaching the new instance temporarily
            std::vector<std::unique_ptr<IEventHandle>> blockedStateResponses;
            auto blockStateResponses = runtime.AddObserver<TEvTxCoordinator::TEvCoordinatorStateResponse>(
                [&](TEvTxCoordinator::TEvCoordinatorStateResponse::TPtr& ev) {
                    Cerr << "... blocking state response from " << ev->Sender << " to " << ev->GetRecipientRewrite() << Endl;
                    Cerr << ev->Get()->Record.DebugString();
                    blockedStateResponses.emplace_back(ev.Release());
                });

            // Rewind to some older time
            runtime.UpdateCurrentTime(oldTimestamp, /* rewind */ true);

            // Start a new bootstrapper, which will boot a new instance in parallel
            Cerr << "... starting a new coordinator instance" << Endl;
            CreateTestBootstrapper(runtime, CreateTestTabletInfo(coordinatorId, TTabletTypes::Coordinator), &CreateFlatTxCoordinator);

            // Wait until new coordinator almost receives the in-memory state
            waitFor([&]{ return blockedStateResponses.size() >= 1; }, "migrated state");

            // Unblock previously blocked blobstorage messages
            // Since new coordinator has started the storage is already blocked
            Cerr << "... unblocking put responses and requests" << Endl;
            for (auto& ev : blockedPutResponses) {
                runtime.Send(ev.release(), 0, true);
            }
            blockedPutResponses.clear();

            // Sleep a little, so everything settles (e.g. committed plan is sent to mediator)
            runtime.SimulateSleep(TDuration::MilliSeconds(1));

            waitFor([&]{ return oldTabletStopped; }, "old tablet stopped");
            hooks.PlannedSteps.clear();

            // Unblock the in-memory state transfer
            blockStateResponses.Remove();
            for (auto& ev : blockedStateResponses) {
                runtime.Send(ev.release(), 0, true);
            }
            blockedStateResponses.clear();

            size_t oldObservedSteps = observedSteps.size();

            // Make sure new requests go to the new instance
            InvalidateTabletResolverCache(runtime, coordinatorId);

            // Plan another persistent transaction
            ui64 persistentTxId2 = 10000011;
            Cerr << "... trying to plan tx " << persistentTxId2 << Endl;
            if (auto propose = std::make_unique<TEvTxProxy::TEvProposeTransaction>(coordinatorId, persistentTxId2, 0, Min<ui64>(), Max<ui64>())) {
                auto* tx = propose->Record.MutableTransaction();
                auto* affected = tx->AddAffectedSet();
                affected->SetTabletId(tabletId);
                affected->SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

                runtime.SendToPipe(coordinatorId, sender, propose.release());
            }

            // Wait until it's actually planned
            waitFor([&]{ return hooks.PlannedSteps.size() >= 1; }, "planned another persistent tx");

            // Wait until mediator observes it
            waitFor([&]{ return observedSteps.size() > oldObservedSteps; }, "new step reaches mediator");

            runtime.SimulateSleep(TDuration::MilliSeconds(50));
            UNIT_ASSERT_C(observedTabletTxs.contains(persistentTxId2),
                "Tablet did not observe a persistent tx " << persistentTxId2);

            Y_UNUSED(sender);
            Y_UNUSED(coordinatorId);
            Y_UNUSED(mediatorId);

            Y_UNUSED(oldTimestamp);
            Y_UNUSED(oldCoordinatorSysActorId);
            Y_UNUSED(oldCoordinatorUserActorId);

            Y_UNUSED(persistentPlanStep);
            Y_UNUSED(volatileEmptyPlanStep);
        }

    } // Y_UNIT_TEST_SUITE(CoordinatorVolatile)

} // namespace NKikimr::NFlatTxCoordinator::NTest
