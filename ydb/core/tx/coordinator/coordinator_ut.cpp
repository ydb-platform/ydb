#include "coordinator_impl.h"
#include "coordinator_hooks.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

namespace NKikimr::NFlatTxCoordinator::NTest {

    using namespace Tests;

    Y_UNIT_TEST_SUITE(Coordinator) {

        Y_UNIT_TEST(ReadStepSubscribe) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(2)
                .SetUseRealThreads(false);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto &runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);

            auto sender = runtime.AllocateEdgeActor();
            ui64 coordinatorId = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

            ui64 lastMediatorStep = 0;
            auto observeMediatorSteps = [&](TAutoPtr<IEventHandle>& ev) -> auto {
                switch (ev->GetTypeRewrite()) {
                    case TEvMediatorQueueStep::EventType: {
                        auto* msg = ev->Get<TEvMediatorQueueStep>();
                        ui64 step = msg->Steps.back().Step;
                        lastMediatorStep = Max(lastMediatorStep, step);
                        break;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto prevObserverFunc = runtime.SetObserverFunc(observeMediatorSteps);

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

            // Wait for the first mediator step
            waitFor([&]{ return lastMediatorStep != 0; }, "the first mediator step");

            const ui64 firstMediatorStep = lastMediatorStep;
            Cerr << "... found first step to be " << firstMediatorStep << Endl;

            // Acquire a new read step, it must be equal to first step,
            // as some shards may have executed transactions with it already
            {
                ForwardToTablet(runtime, coordinatorId, sender, new TEvTxProxy::TEvAcquireReadStep(coordinatorId));
                auto ev = runtime.GrabEdgeEvent<TEvTxProxy::TEvAcquireReadStepResult>(sender);
                ui64 readStep = ev->Get()->Record.GetStep();
                Cerr << "... acquired read step " << readStep << Endl;
                UNIT_ASSERT_VALUES_EQUAL(readStep, firstMediatorStep);
            }

            // Wait for the next mediator step
            waitFor([&]{ return lastMediatorStep > firstMediatorStep; }, "the next mediator step");

            const ui64 secondMediatorStep = lastMediatorStep;
            Cerr << "... found second step to be " << secondMediatorStep << Endl;

            // Subscribe to mediator step updates
            {
                ForwardToTablet(runtime, coordinatorId, sender, new TEvTxProxy::TEvSubscribeReadStep(coordinatorId, /* seqno */ 1));
                auto ev = runtime.GrabEdgeEvent<TEvTxProxy::TEvSubscribeReadStepResult>(sender);
                auto* msg = ev->Get();
                Cerr << "... read step subscribe result: [" << msg->Record.GetLastAcquireStep() << ", " << msg->Record.GetNextAcquireStep() << "]" << Endl;
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLastAcquireStep(), firstMediatorStep);
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetNextAcquireStep(), secondMediatorStep);
            }

            // Wait for the next update
            ui64 nextReadStep;
            {
                auto ev = runtime.GrabEdgeEvent<TEvTxProxy::TEvSubscribeReadStepUpdate>(sender);
                auto* msg = ev->Get();
                Cerr << "... read step subscribe update: " << msg->Record.GetNextAcquireStep() << Endl;
                UNIT_ASSERT_GT(msg->Record.GetNextAcquireStep(), secondMediatorStep);
                // Note, we cannot compare with lastMediatorStep here, because mediator may have not seen it yet
                // UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetNextAcquireStep(), lastMediatorStep);
                nextReadStep = msg->Record.GetNextAcquireStep();
            }

            // Acquire a new read step, it must be equal to predicted
            {
                ForwardToTablet(runtime, coordinatorId, sender, new TEvTxProxy::TEvAcquireReadStep(coordinatorId));
                auto ev = runtime.GrabEdgeEvent<TEvTxProxy::TEvAcquireReadStepResult>(sender);
                ui64 readStep = ev->Get()->Record.GetStep();
                Cerr << "... acquired read step " << readStep << Endl;
                UNIT_ASSERT_VALUES_EQUAL(readStep, nextReadStep);
            }

            // Subscribe using pipes
            auto sender2 = runtime.AllocateEdgeActor();
            auto pipe2 = runtime.ConnectToPipe(coordinatorId, sender2, 0, GetPipeConfigWithRetries());
            {
                runtime.SendToPipe(pipe2, sender2, new TEvTxProxy::TEvSubscribeReadStep(coordinatorId, /* seqno */ 42));
                auto ev = runtime.GrabEdgeEvent<TEvTxProxy::TEvSubscribeReadStepResult>(sender2);
                auto* msg = ev->Get();
                Cerr << "... read step subscribe result: [" << msg->Record.GetLastAcquireStep() << ", " << msg->Record.GetNextAcquireStep() << "]" << Endl;
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 42u);
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetLastAcquireStep(), nextReadStep);
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetNextAcquireStep(), nextReadStep);
            }

            // Wait for the next update
            {
                auto ev = runtime.GrabEdgeEvent<TEvTxProxy::TEvSubscribeReadStepUpdate>(sender2);
                auto* msg = ev->Get();
                Cerr << "... read step subscribe update: " << msg->Record.GetNextAcquireStep() << Endl;
                UNIT_ASSERT_GT(msg->Record.GetNextAcquireStep(), nextReadStep);
            }

            // Close the pipe, we expect to unsubscribe automatically
            runtime.ClosePipe(pipe2, sender2, 0);

            // Verify that we only receive updates for the first subscriber
            for (int i = 0; i < 5; ++i) {
                TAutoPtr<IEventHandle> handle;
                auto* msg = runtime.GrabEdgeEventRethrow<TEvTxProxy::TEvSubscribeReadStepUpdate>(handle);
                Cerr << "... read step subscribe update: " << msg->Record.GetNextAcquireStep() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            }

            // Subscribe using an actor from a different node (without pipes)
            auto sender3 = runtime.AllocateEdgeActor(1);
            {
                ForwardToTablet(runtime, coordinatorId, sender3, new TEvTxProxy::TEvSubscribeReadStep(coordinatorId, /* seqno */ 51), 1);
                auto ev = runtime.GrabEdgeEvent<TEvTxProxy::TEvSubscribeReadStepResult>(sender3);
                auto* msg = ev->Get();
                Cerr << "... read step subscribe result: [" << msg->Record.GetLastAcquireStep() << ", " << msg->Record.GetNextAcquireStep() << "]" << Endl;
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 51u);
            }

            // Simulate a disconnection between nodes (should unsubscribe implicitly)
            runtime.DisconnectNodes(0, 1);

            // Verify that we only receive updates for the first subscriber
            for (int i = 0; i < 5; ++i) {
                TAutoPtr<IEventHandle> handle;
                auto* msg = runtime.GrabEdgeEventRethrow<TEvTxProxy::TEvSubscribeReadStepUpdate>(handle);
                Cerr << "... read step subscribe update: " << msg->Record.GetNextAcquireStep() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            }
        }

        class TEventSinkActor : public TActor<TEventSinkActor> {
        public:
            TEventSinkActor(std::deque<std::unique_ptr<IEventHandle>>& events)
                : TActor(&TThis::Handle)
                , Events(events)
            { }

            template<class TEvent>
            static typename TEvent::TPtr Wait(TTestActorRuntimeBase& runtime, std::deque<std::unique_ptr<IEventHandle>>& events, int attempts = 1) {
                for (int i = 0; i < attempts && events.empty(); ++i) {
                    TDispatchOptions options;
                    options.CustomFinalCondition = [&]() {
                        return !events.empty();
                    };
                    runtime.DispatchEvents(options);
                }

                UNIT_ASSERT_C(!events.empty(), "Expected event " << TypeName<TEvent>() << " was not delivered");
                UNIT_ASSERT_C(events.front()->GetTypeRewrite() == TEvent::EventType,
                    "Expected event " << TypeName<TEvent>()
                    << " but received " << events.front()->ToString());

                auto ev = std::move(events.front());
                events.pop_front();
                return typename TEvent::TPtr(static_cast<TEventHandle<TEvent>*>(ev.release()));
            }

        private:
            void Handle(TAutoPtr<IEventHandle>& ev) {
                Events.emplace_back(ev.Release());
            }

        private:
            std::deque<std::unique_ptr<IEventHandle>>& Events;
        };

        Y_UNIT_TEST(LastStepSubscribe) {
            std::deque<std::unique_ptr<IEventHandle>> events1;
            std::deque<std::unique_ptr<IEventHandle>> events2;

            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(2)
                .SetUseRealThreads(false);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto &runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);

            ui64 coordinatorId = ChangeStateStorage(Coordinator, server->GetSettings().Domain);

            auto sender1 = runtime.Register(new TEventSinkActor(events1), 1);
            auto sender2 = runtime.Register(new TEventSinkActor(events2), 1);
            auto pipe1 = runtime.ConnectToPipe(coordinatorId, {}, 1, {});
            auto pipe2 = runtime.ConnectToPipe(coordinatorId, {}, 1, {});

            // Subscribe the first client
            runtime.SendToPipe(pipe1, sender1, new TEvTxProxy::TEvSubscribeLastStep(coordinatorId, 123), 1, 234);

            ui64 lastStep;
            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events1);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetCoordinatorID(), coordinatorId);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetSeqNo(), 123u);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
                lastStep = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_C(lastStep > 0, lastStep);
            }

            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events1);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetCoordinatorID(), coordinatorId);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetSeqNo(), 123u);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
                ui64 step = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_C(step > lastStep, "step: " << step << " lastStep: " << lastStep);
                lastStep = step;
            }

            UNIT_ASSERT(events2.empty());

            // Subscribe the second client
            runtime.SendToPipe(pipe2, sender2, new TEvTxProxy::TEvSubscribeLastStep(coordinatorId, 234), 1, 345);

            // We expect new subscriptions to see the same step
            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events2);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetCoordinatorID(), coordinatorId);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetSeqNo(), 234u);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 345u);
                ui64 step = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_VALUES_EQUAL(step, lastStep);
            }

            UNIT_ASSERT(events1.empty());

            // Both subscribers should receive the next step
            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events1);
                ui64 step = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_C(step > lastStep, "step: " << step << " lastStep: " << lastStep);
                lastStep = step;
            }

            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events2);
                ui64 step = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_VALUES_EQUAL(step, lastStep);
            }

            // Resubscribe the first client using a greater seqno
            runtime.SendToPipe(pipe1, sender1, new TEvTxProxy::TEvSubscribeLastStep(coordinatorId, 124), 1, 245);
            runtime.SimulateSleep(TDuration::MicroSeconds(1));
            UNIT_ASSERT(events1.size() == 1);
            UNIT_ASSERT(events2.size() == 0);

            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events1);
                ui64 step = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_VALUES_EQUAL(step, lastStep);
            }

            // Subscriptions using a lower seqno must be ignored
            runtime.SendToPipe(pipe1, sender1, new TEvTxProxy::TEvSubscribeLastStep(coordinatorId, 123), 1, 234);
            runtime.SimulateSleep(TDuration::MicroSeconds(1));
            UNIT_ASSERT(events1.size() == 0);
            UNIT_ASSERT(events2.size() == 0);

            // Unsubscribe using a lower seqno must be ignored
            runtime.SendToPipe(pipe1, sender1, new TEvTxProxy::TEvUnsubscribeLastStep(coordinatorId, 123), 1, 0);

            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events1);
                ui64 step = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_C(step > lastStep, "step: " << step << " lastStep: " << lastStep);
                lastStep = step;
            }

            {
                auto ev = TEventSinkActor::Wait<TEvTxProxy::TEvUpdatedLastStep>(runtime, events2);
                ui64 step = ev->Get()->Record.GetLastStep();
                UNIT_ASSERT_VALUES_EQUAL(step, lastStep);
            }

            // Unsubscribe the first client
            runtime.SendToPipe(pipe1, sender1, new TEvTxProxy::TEvUnsubscribeLastStep(coordinatorId, 124), 1, 0);

            // We expect the second client to receive updates, but not the first one
            runtime.SimulateSleep(TDuration::Seconds(2));
            UNIT_ASSERT(events1.size() == 0);
            UNIT_ASSERT(events2.size() > 0);
            events2.clear();

            // Close the second pipe
            runtime.ClosePipe(pipe2, {}, 1);

            // We expect the second client to stop receiving updates as well
            runtime.SimulateSleep(TDuration::Seconds(2));
            UNIT_ASSERT(events1.size() == 0);
            UNIT_ASSERT(events2.size() == 0);
        }

        Y_UNIT_TEST(RestoreDomainConfiguration) {
            struct TCoordinatorHooks : public ICoordinatorHooks {
                bool AllowPersistConfig_ = false;
                std::vector<std::pair<ui64, ui64>> PersistConfig_;

                bool PersistConfig(ui64 tabletId, const NKikimrSubDomains::TProcessingParams& config) override {
                    PersistConfig_.emplace_back(tabletId, config.GetVersion());
                    return AllowPersistConfig_;
                }
            } hooks;
            TCoordinatorHooksGuard hooksGuard(hooks);

            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(1)
                .SetUseRealThreads(false);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto &runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);

            auto sender = runtime.AllocateEdgeActor();
            ui64 coordinatorId = ChangeStateStorage(Coordinator, server->GetSettings().Domain);
            runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT_C(hooks.PersistConfig_.size() > 0,
                "Coordinators didn't even try to persist configs");

            Cerr << "Rebooting coordinator to restore config" << Endl;
            hooks.AllowPersistConfig_ = true;
            hooks.PersistConfig_.clear();
            RebootTablet(runtime, coordinatorId, sender);
            runtime.SimulateSleep(TDuration::Seconds(1));

            UNIT_ASSERT_C(hooks.PersistConfig_.size() == 1,
                "Unexpected number of PersistConfig events: " << hooks.PersistConfig_.size());
            UNIT_ASSERT_VALUES_EQUAL(hooks.PersistConfig_[0].first, coordinatorId);
            UNIT_ASSERT_VALUES_EQUAL(hooks.PersistConfig_[0].second, 0u);

            Cerr << "Rebooting coordinator a second time" << Endl;
            hooks.PersistConfig_.clear();
            RebootTablet(runtime, coordinatorId, sender);
            runtime.SimulateSleep(TDuration::Seconds(1));

            UNIT_ASSERT_C(hooks.PersistConfig_.empty(), "Unexpected PersistConfig attempt");
        }

        using TEvCreateDatabaseRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
            Ydb::Cms::CreateDatabaseRequest,
            Ydb::Cms::CreateDatabaseResponse>;

        Y_UNIT_TEST(RestoreTenantConfiguration) {
            struct TCoordinatorHooks : public ICoordinatorHooks {
                bool AllowPersistConfig_ = true;
                std::unordered_map<ui64, NKikimrSubDomains::TProcessingParams> PersistConfig_;

                bool PersistConfig(ui64 tabletId, const NKikimrSubDomains::TProcessingParams& config) override {
                    PersistConfig_[tabletId] = config;
                    return AllowPersistConfig_;
                }
            } hooks;
            TCoordinatorHooksGuard hooksGuard(hooks);

            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(1)
                .SetDynamicNodeCount(4)
                .SetUseRealThreads(false)
                .AddStoragePoolType("ssd");

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto& runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::CMS_TENANTS, NActors::NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
            auto sender = runtime.AllocateEdgeActor();

            Tests::TTenants tenants(server);

            runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT_C(hooks.PersistConfig_.size() > 0, "Expected root coordinators to persist configs");

            hooks.AllowPersistConfig_ = false;
            hooks.PersistConfig_.clear();

            auto createDatabase = [&]() {
                Ydb::Cms::CreateDatabaseRequest request;
                request.set_path("/Root/db1");
                auto* resources = request.mutable_resources();
                auto* storage = resources->add_storage_units();
                storage->set_unit_kind("ssd");
                storage->set_count(1);
                Cerr << (TStringBuilder() << "Sending CreateDatabase request" << Endl);
                auto future = NRpcService::DoLocalRpc<TEvCreateDatabaseRequest>(
                    std::move(request), "", "", runtime.GetActorSystem(0));
                auto response = runtime.WaitFuture(std::move(future));
                Cerr << (TStringBuilder() << "Got CreateDatabase response:\n" << response.DebugString());
                return response;
            };

            // NOTE: local rpc forces sync mode
            auto createDatabaseResponse = createDatabase();
            UNIT_ASSERT(createDatabaseResponse.operation().ready());
            UNIT_ASSERT_VALUES_EQUAL(createDatabaseResponse.operation().status(), Ydb::StatusIds::SUCCESS);

            // runtime.SimulateSleep(TDuration::Seconds(1));
            UNIT_ASSERT_C(hooks.PersistConfig_.empty(), "Unexpected PersistConfig without a running tenant");

            Cerr << (TStringBuilder() << "Starting a database tenant" << Endl);
            tenants.Run("/Root/db1", 1);

            Cerr << (TStringBuilder() << "Sleeping for tenant to start" << Endl);
            runtime.SimulateSleep(TDuration::Seconds(5));
            UNIT_ASSERT_C(hooks.PersistConfig_.size() > 0, "Expected coordinators to attempt to persist configs");
            std::vector<ui64> coordinators;
            for (auto& pr : hooks.PersistConfig_) {
                UNIT_ASSERT_C(pr.second.GetVersion() == 2,
                    "Expected tenant coordinator to have a version 2 config:\n" << pr.second.DebugString());
                coordinators.push_back(pr.first);
            }

            Cerr << (TStringBuilder() << "Rebooting coordinators to restore configs" << Endl);
            hooks.AllowPersistConfig_ = true;
            hooks.PersistConfig_.clear();
            for (ui64 coordinatorId : coordinators) {
                RebootTablet(runtime, coordinatorId, sender);
            }

            runtime.SimulateSleep(TDuration::MilliSeconds(50));
            UNIT_ASSERT_C(hooks.PersistConfig_.size() == coordinators.size(), "Expected all coordinators to persist restored config");
            for (auto& pr : hooks.PersistConfig_) {
                UNIT_ASSERT_C(pr.second.GetVersion() == 2,
                    "Expected tenant coordinator to restore a version 2 config:\n" << pr.second.DebugString());
            }

            // runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
            Cerr << (TStringBuilder() << "Rebooting coordinators a second time" << Endl);
            hooks.PersistConfig_.clear();
            for (ui64 coordinatorId : coordinators) {
                Cerr << (TStringBuilder() << TInstant::Now() << " Rebooting coordinator " << coordinatorId << Endl);
                RebootTablet(runtime, coordinatorId, sender);
            }
            Cerr << (TStringBuilder() << TInstant::Now() << " Finished rebooting coordinators a second time" << Endl);

            runtime.SimulateSleep(TDuration::MilliSeconds(50));
            UNIT_ASSERT_C(hooks.PersistConfig_.empty(), "Unexpected persist attempt after a second reboot");
        }

        Y_UNIT_TEST(LastEmptyStepResent) {
            TPortManager pm;
            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetNodeCount(1)
                .SetUseRealThreads(false);

            Tests::TServer::TPtr server = new TServer(serverSettings);

            auto &runtime = *server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);

            auto sender = runtime.AllocateEdgeActor();
            ui64 mediatorId = ChangeStateStorage(Mediator, server->GetSettings().Domain);
            runtime.SimulateSleep(TDuration::Seconds(1));

            std::vector<ui64> emptySteps;
            auto stepsObserver = [&](auto& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvTxCoordinator::TEvCoordinatorStep::EventType: {
                        auto* msg = ev->template Get<TEvTxCoordinator::TEvCoordinatorStep>();
                        ui64 step = msg->Record.GetStep();
                        bool empty = msg->Record.TransactionsSize() == 0;
                        Cerr << "... observed " << step << ": " << (empty ? "empty" : "not empty") << Endl;
                        if (empty) {
                            emptySteps.push_back(step);
                        }
                        break;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            auto prevObserverFunc = runtime.SetObserverFunc(stepsObserver);

            runtime.SimulateSleep(TDuration::Seconds(10));
            UNIT_ASSERT_C(emptySteps.size() > 1, "Expected multiple empty steps, not " << emptySteps.size());
            ui64 lastObserved = emptySteps.back();
            emptySteps.clear();

            RebootTablet(runtime, mediatorId, sender);
            if (emptySteps.empty()) {
                Cerr << "... waiting for empty steps" << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return !emptySteps.empty();
                };
                runtime.DispatchEvents(options);
            }
            UNIT_ASSERT_C(!emptySteps.empty(), "Expected at least one empty step");
            UNIT_ASSERT_C(emptySteps.front() == lastObserved,
                "Expected to observe " << lastObserved << " empty step, not " << emptySteps.front());
        }

    } // Y_UNIT_TEST_SUITE(Coordinator)


} // namespace NKikimr::NTxCoordinator::NTest
