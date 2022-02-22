#include "time_cast.h"
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

            auto &runtime = *server->GetRuntime();
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

    } // Y_UNIT_TEST_SUITE(MediatorTimeCast)

} // namespace NKikimr::NMediatorTimeCastTest
