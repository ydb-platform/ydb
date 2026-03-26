#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/persqueue/public/cloud_events/cloud_events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/blocking_queue/blocking_queue.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NPQ::NCloudEvents;

namespace {

using TCloudEventQueue = NThreading::TBlockingQueue<TCloudEventInfo>;
using TCloudEventQueuePtr = std::shared_ptr<TCloudEventQueue>;

static void AssertCloudEventInfo(const TCloudEventInfo& info,
                                 const TString& expectedEventType,
                                 const TString& expectedPath) {
    UNIT_ASSERT_STRINGS_EQUAL(GetCloudEventType(info), expectedEventType);
    UNIT_ASSERT_STRINGS_EQUAL(info.TopicPath, expectedPath);
}

static void PopAndAssertTwoEvents(TCloudEventQueue& queue,
                                 const TString& expectedType1,
                                 const TString& expectedType2,
                                 const TString& expectedPath,
                                 TDuration timeout) {
    auto ev1 = queue.Pop(timeout);
    UNIT_ASSERT_C(ev1.Defined(), "Expected first cloud event");
    auto ev2 = queue.Pop(timeout);
    UNIT_ASSERT_C(ev2.Defined(), "Expected second cloud event");

    TString type1 = GetCloudEventType(*ev1);
    TString type2 = GetCloudEventType(*ev2);

    auto check = [&](const TString& type, const TCloudEventInfo& info) {
        if (type == expectedType1 || type == expectedType2) {
            AssertCloudEventInfo(info, type, expectedPath);
            return true;
        }
        return false;
    };
    UNIT_ASSERT_C(check(type1, *ev1), "Unexpected event type: " << type1);
    UNIT_ASSERT_C(check(type2, *ev2), "Unexpected event type: " << type2);
    UNIT_ASSERT_C(type1 != type2, "Expected different event types, got " << type1 << " twice");
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeShardTopicCloudEvents) {

    Y_UNIT_TEST(CreateTopicCloudEvent) {
        TTestBasicRuntime runtime;
        auto cloudEventQueue = std::make_shared<TCloudEventQueue>(0);

        runtime.SetEventFilter([queue = cloudEventQueue](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NPQ::NEvents::InternalEventSpaceBegin(NPQ::NEvents::EServices::CLOUD_EVENTS)) {
                auto* cloudEv = ev->Get<NPQ::NCloudEvents::TCloudEvent>();
                if (cloudEv) {
                    queue->Push(cloudEv->Info);
                    return true; // drop event (don't deliver to actor)
                }
            }
            return false; // deliver other events
        });

        TTestEnv env(runtime);
        ui64 txId = 1000;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
            R"(
                Name: "MyTopic"
                TotalGroupCount: 4
                PartitionPerTablet: 2
                PQTabletConfig { PartitionConfig { LifetimeSeconds: 10 } }
            )");
        env.TestWaitNotification(runtime, txId);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto info = cloudEventQueue->Pop(TDuration::Seconds(5));
        UNIT_ASSERT_C(info.Defined(), "Expected cloud event from TCloudEventsActor");

        AssertCloudEventInfo(*info,
            "yandex.cloud.events.ydb.topics.CreateTopic",
            "/MyRoot/MyTopic");
    }

    Y_UNIT_TEST(AlterTopicCloudEvent) {
        TTestBasicRuntime runtime;
        auto cloudEventQueue = std::make_shared<TCloudEventQueue>(0);

        runtime.SetEventFilter([queue = cloudEventQueue](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NPQ::NEvents::InternalEventSpaceBegin(NPQ::NEvents::EServices::CLOUD_EVENTS)) {
                auto* cloudEv = ev->Get<NPQ::NCloudEvents::TCloudEvent>();
                if (cloudEv) {
                    queue->Push(cloudEv->Info);
                    return true; // drop event
                }
            }
            return false;
        });

        TTestEnv env(runtime);
        ui64 txId = 1000;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
            R"(
                Name: "MyTopic"
                TotalGroupCount: 4
                PartitionPerTablet: 2
                PQTabletConfig { PartitionConfig { LifetimeSeconds: 10 } }
            )");
        env.TestWaitNotification(runtime, txId);

        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
            R"(
                Name: "MyTopic"
                TotalGroupCount: 6
                PartitionPerTablet: 2
                PQTabletConfig { PartitionConfig { LifetimeSeconds: 42 } }
            )");
        env.TestWaitNotification(runtime, txId);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(3));

        PopAndAssertTwoEvents(*cloudEventQueue,
            "yandex.cloud.events.ydb.topics.CreateTopic",
            "yandex.cloud.events.ydb.topics.AlterTopic",
            "/MyRoot/MyTopic",
            TDuration::Seconds(10));
    }

    Y_UNIT_TEST(DropTopicCloudEvent) {
        TTestBasicRuntime runtime;
        auto cloudEventQueue = std::make_shared<TCloudEventQueue>(0);

        runtime.SetEventFilter([queue = cloudEventQueue](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NPQ::NEvents::InternalEventSpaceBegin(NPQ::NEvents::EServices::CLOUD_EVENTS)) {
                auto* cloudEv = ev->Get<NPQ::NCloudEvents::TCloudEvent>();
                if (cloudEv) {
                    queue->Push(cloudEv->Info);
                    return true; // drop event
                }
            }
            return false;
        });

        TTestEnv env(runtime);
        ui64 txId = 1000;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
            R"(
                Name: "MyTopic"
                TotalGroupCount: 4
                PartitionPerTablet: 2
                PQTabletConfig { PartitionConfig { LifetimeSeconds: 10 } }
            )");
        env.TestWaitNotification(runtime, txId);

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "MyTopic");
        env.TestWaitNotification(runtime, txId);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(3));

        PopAndAssertTwoEvents(*cloudEventQueue,
            "yandex.cloud.events.ydb.topics.CreateTopic",
            "yandex.cloud.events.ydb.topics.DeleteTopic",
            "/MyRoot/MyTopic",
            TDuration::Seconds(10));
    }
}
