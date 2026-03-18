#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/persqueue/public/cloud_events/actor.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/blocking_queue/blocking_queue.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;
using namespace NPQ::NCloudEvents;

namespace {

using TCloudEventQueue = NThreading::TBlockingQueue<TString>;
using TCloudEventQueuePtr = std::shared_ptr<TCloudEventQueue>;

static NJson::TJsonValue ParseCloudEventJson(const TString& json) {
    NJson::TJsonValue cloudEvent;
    UNIT_ASSERT_C(NJson::ReadJsonTree(json, &cloudEvent), "Failed to parse cloud event JSON: " << json);
    return cloudEvent;
}

static void AssertCloudEventFields(const NJson::TJsonValue& cloudEvent,
                                   const TString& expectedEventType,
                                   const TString& expectedPath) {
    const auto* eventMetadata = cloudEvent.GetValueByPath("event_metadata");
    UNIT_ASSERT_C(eventMetadata != nullptr, "Missing event_metadata");
    UNIT_ASSERT_STRINGS_EQUAL((*eventMetadata)["event_type"].GetString(), expectedEventType);

    const auto* details = cloudEvent.GetValueByPath("details");
    UNIT_ASSERT_C(details != nullptr, "Missing details");
    UNIT_ASSERT_STRINGS_EQUAL((*details)["path"].GetString(), expectedPath);
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

    auto json1 = ParseCloudEventJson(*ev1);
    auto json2 = ParseCloudEventJson(*ev2);

    const auto* meta1 = json1.GetValueByPath("event_metadata");
    const auto* meta2 = json2.GetValueByPath("event_metadata");
    UNIT_ASSERT_C(meta1 != nullptr, "Missing event_metadata in first event");
    UNIT_ASSERT_C(meta2 != nullptr, "Missing event_metadata in second event");

    TString type1 = (*meta1)["event_type"].GetString();
    TString type2 = (*meta2)["event_type"].GetString();

    auto check = [&](const TString& type, const NJson::TJsonValue& j) {
        if (type == expectedType1 || type == expectedType2) {
            AssertCloudEventFields(j, type, expectedPath);
            return true;
        }
        return false;
    };
    UNIT_ASSERT_C(check(type1, json1), "Unexpected event type: " << type1);
    UNIT_ASSERT_C(check(type2, json2), "Unexpected event type: " << type2);
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
                    TString json = BuildTopicCloudEventJson(cloudEv->Info);
                    queue->Push(std::move(json));
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

        auto json = cloudEventQueue->Pop(TDuration::Seconds(5));
        UNIT_ASSERT_C(json.Defined(), "Expected cloud event from TCloudEventsActor");

        auto cloudEvent = ParseCloudEventJson(*json);
        AssertCloudEventFields(cloudEvent,
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
                    TString json = BuildTopicCloudEventJson(cloudEv->Info);
                    queue->Push(std::move(json));
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
                    TString json = BuildTopicCloudEventJson(cloudEv->Info);
                    queue->Push(std::move(json));
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
