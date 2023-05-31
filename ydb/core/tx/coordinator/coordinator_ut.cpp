#include "coordinator_impl.h"
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTxCoordinator::NTest {

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
            auto observeMediatorSteps = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
                switch (ev->GetTypeRewrite()) {
                    case TEvTxCoordinator::TEvMediatorQueueStep::EventType: {
                        auto* msg = ev->Get<TEvTxCoordinator::TEvMediatorQueueStep>();
                        ui64 step = msg->Step->Step;
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

    } // Y_UNIT_TEST_SUITE(Coordinator)


} // namespace NKikimr::NTxCoordinator::NTest
