#include <ydb/core/persqueue/pqrb/topic_configuration_replicator_factory.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TTopicConfigurationReplicatorTests) {

Y_UNIT_TEST(CompletesWithoutReplicatingConfiguration) {
    TTestBasicRuntime runtime(1, false);
    TAppPrepare prepare;
    runtime.Initialize(prepare.Unwrap());
    const auto parent = runtime.AllocateEdgeActor();
    NKikimrPQ::TMirrorPartitionConfig config;
    config.SetTopic("source-topic");
    const auto replicator = runtime.Register(CreateTopicConfigurationReplicator(42, parent, config));
    runtime.EnableScheduleForActor(replicator);
    size_t credentialsRequests = 0;
    size_t completionNotifications = 0;
    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->Sender == replicator) {
            credentialsRequests += event->GetTypeRewrite() == TEvPQ::TEvInitCredentials::EventType;
            completionNotifications += event->GetTypeRewrite() == TEvPQ::TEvTopicConfigurationReplicationFinished::EventType;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });
    auto completion = runtime.GrabEdgeEvent<TEvPQ::TEvTopicConfigurationReplicationFinished>(parent);
    UNIT_ASSERT_VALUES_EQUAL(completion->Sender, replicator);
    runtime.SimulateSleep(TDuration::Seconds(6));
    UNIT_ASSERT(runtime.FindActor(replicator));
    UNIT_ASSERT_VALUES_EQUAL(credentialsRequests, 0);
    UNIT_ASSERT_VALUES_EQUAL(completionNotifications, 2);
    runtime.Send(new IEventHandle(replicator, parent, new TEvents::TEvPoisonPill()), 0, true);
    runtime.SimulateSleep(TDuration::Seconds(6));
    UNIT_ASSERT(!runtime.FindActor(replicator));
    UNIT_ASSERT_VALUES_EQUAL(completionNotifications, 2);
    runtime.SetObserverFunc(TTestActorRuntimeBase::DefaultObserverFunc);
}

} // Y_UNIT_TEST_SUITE(TTopicConfigurationReplicatorTests)

} // namespace NKikimr::NPQ
