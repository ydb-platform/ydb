#include <ydb/core/persqueue/public/cloud_events/cloud_events.h>
#include <ydb/core/persqueue/public/cloud_events/actor.h>
#include <ydb/core/persqueue/public/cloud_events/proto/topics.pb.h>

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <google/protobuf/util/json_util.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <memory>
#include <util/generic/maybe.h>

namespace NKikimr::NPQ::NCloudEvents {

using namespace NPersQueue;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic::NTests;

static TCloudEventInfo MakeCreateTopicEventInfo(const TString& topicPath = "/root/db/topic1") {
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup);
    modifyScheme.MutableCreatePersQueueGroup();

    TCloudEventInfo info;
    info.CloudId = "cloud1";
    info.FolderId = "folder1";
    info.TopicPath = topicPath;
    info.Issue = "";
    info.UserSID = "user@iam";
    info.RemoteAddress = "127.0.0.1";
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = std::move(modifyScheme);
    info.OperationStatus = NKikimrScheme::StatusSuccess;
    return info;
}

static TCloudEventInfo MakeAlterTopicEventInfo(const TString& topicPath = "/root/db/topic1") {
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup);

    auto* alter = modifyScheme.MutableAlterPersQueueGroup();
    alter->SetName("topic1");

    auto* pqConfig = alter->MutablePQTabletConfig();
    auto* partitionStrategy = pqConfig->MutablePartitionStrategy();
    partitionStrategy->SetMinPartitionCount(2);
    partitionStrategy->SetMaxPartitionCount(5);
    partitionStrategy->SetPartitionStrategyType(NKikimrPQ::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);

    auto* partitionConfig = pqConfig->MutablePartitionConfig();
    partitionConfig->SetWriteSpeedInBytesPerSecond(123456);
    partitionConfig->SetLifetimeSeconds(3600);
    partitionConfig->SetStorageLimitBytes(50_MB);

    pqConfig->SetMeteringMode(NKikimrPQ::TPQTabletConfig_EMeteringMode_METERING_MODE_REQUEST_UNITS);
    pqConfig->SetMetricsLevel(2);

    auto* consumer = pqConfig->AddConsumers();
    consumer->SetName("consumer-1");

    TCloudEventInfo info;
    info.CloudId = "cloud1";
    info.FolderId = "folder1";
    info.TopicPath = topicPath;
    info.Issue = "";
    info.UserSID = "user@iam";
    info.RemoteAddress = "127.0.0.1";
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = std::move(modifyScheme);
    info.OperationStatus = NKikimrScheme::StatusSuccess;
    return info;
}

static NJson::TJsonValue ParseCloudEventProtobuf(const TString& data) {
    using namespace yandex::cloud::events::ydb::topics;
    for (const auto* msg : {static_cast<const google::protobuf::Message*>(static_cast<const CreateTopic*>(nullptr)),
                           static_cast<const google::protobuf::Message*>(static_cast<const AlterTopic*>(nullptr)),
                           static_cast<const google::protobuf::Message*>(static_cast<const DeleteTopic*>(nullptr))}) {
        (void)msg;
    }
    CreateTopic createEv;
    if (createEv.ParseFromString(data) && createEv.event_metadata().event_type().find("CreateTopic") != TString::npos) {
        TString json;
        google::protobuf::util::JsonPrintOptions opts;
        opts.preserve_proto_field_names = true;
        opts.always_print_primitive_fields = true;
        Y_ABORT_UNLESS(google::protobuf::util::MessageToJsonString(createEv, &json, opts).ok());
        NJson::TJsonValue out;
        UNIT_ASSERT_C(NJson::ReadJsonTree(json, &out), "MessageToJsonString ok but ReadJsonTree failed");
        return out;
    }
    AlterTopic alterEv;
    if (alterEv.ParseFromString(data) && alterEv.event_metadata().event_type().find("AlterTopic") != TString::npos) {
        TString json;
        google::protobuf::util::JsonPrintOptions opts;
        opts.preserve_proto_field_names = true;
        opts.always_print_primitive_fields = true;
        Y_ABORT_UNLESS(google::protobuf::util::MessageToJsonString(alterEv, &json, opts).ok());
        NJson::TJsonValue out;
        UNIT_ASSERT_C(NJson::ReadJsonTree(json, &out), "MessageToJsonString ok but ReadJsonTree failed");
        return out;
    }
    DeleteTopic deleteEv;
    UNIT_ASSERT_C(deleteEv.ParseFromString(data) && deleteEv.event_metadata().event_type().find("DeleteTopic") != TString::npos,
        "Failed to parse as CreateTopic, AlterTopic, or DeleteTopic protobuf");
    TString json;
    google::protobuf::util::JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    opts.always_print_primitive_fields = true;
    Y_ABORT_UNLESS(google::protobuf::util::MessageToJsonString(deleteEv, &json, opts).ok());
    NJson::TJsonValue out;
    UNIT_ASSERT_C(NJson::ReadJsonTree(json, &out), "MessageToJsonString ok but ReadJsonTree failed");
    return out;
}

static void AssertCloudEventJsonStructure(const NJson::TJsonValue& cloudEvent, const TString& expectedEventType, const TString& expectedPath) {
    const auto* eventMetadata = cloudEvent.GetValueByPath("event_metadata");
    UNIT_ASSERT_C(eventMetadata != nullptr, "Missing event_metadata");
    UNIT_ASSERT_STRINGS_EQUAL((*eventMetadata)["event_type"].GetString(), expectedEventType);

    const auto* details = cloudEvent.GetValueByPath("details");
    UNIT_ASSERT_C(details != nullptr, "Missing details");
    UNIT_ASSERT_STRINGS_EQUAL((*details)["path"].GetString(), expectedPath);

    const auto* auth = cloudEvent.GetValueByPath("authentication");
    UNIT_ASSERT_C(auth != nullptr, "Missing authentication");
    UNIT_ASSERT_STRINGS_EQUAL((*auth)["subject_id"].GetString(), "user@iam");

    const auto* evMetadata = cloudEvent.GetValueByPath("event_metadata");
    UNIT_ASSERT_C(evMetadata != nullptr, "Missing event_metadata");
    UNIT_ASSERT(evMetadata->GetMap().find("cloud_id") != evMetadata->GetMap().end());
    UNIT_ASSERT(evMetadata->GetMap().find("folder_id") != evMetadata->GetMap().end());
}

static TCloudEventInfo MakeDeleteTopicEventInfo(const TString& topicPath = "/root/db/topic1") {
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
    modifyScheme.MutableDrop()->SetName(topicPath);

    TCloudEventInfo info;
    info.CloudId = "cloud1";
    info.FolderId = "folder1";
    info.TopicPath = topicPath;
    info.Issue = "";
    info.UserSID = "user@iam";
    info.RemoteAddress = "127.0.0.1";
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = std::move(modifyScheme);
    info.OperationStatus = NKikimrScheme::StatusSuccess;
    return info;
}

class TInMemoryEventsWriter final : public IEventsWriter {
public:
    explicit TInMemoryEventsWriter(std::shared_ptr<TVector<TString>> events)
        : Events(std::move(events))
    {}

    void Write(const TString& data) override {
        Events->push_back(data);
    }

private:
    std::shared_ptr<TVector<TString>> Events;
};


class TFakeUaEventsSession final : public IUaEventsSession {
public:
    struct TState {
        TVector<TString> Sent;
        ui32 CloseCalls = 0;
    };

    explicit TFakeUaEventsSession(std::shared_ptr<TState> state)
        : State(std::move(state))
    {}

    void Send(const TString& data) override {
        State->Sent.push_back(data);
    }

    void Close() override {
        ++State->CloseCalls;
    }

private:
    std::shared_ptr<TState> State;
};

Y_UNIT_TEST_SUITE(CloudEventsAuditTest) {
    Y_UNIT_TEST(CreateTopicEventAudit) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        setup->GetServer().EnableLogs(
            {NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY},
            NActors::NLog::PRI_INFO
        );

        auto events = std::make_shared<TVector<TString>>();
        auto writer = MakeHolder<TInMemoryEventsWriter>(events);

        auto& runtime = setup->GetRuntime();
        auto edgeId = runtime.AllocateEdgeActor();
        auto actorId = runtime.Register(new TCloudEventsActor(std::move(writer)));
        runtime.EnableScheduleForActor(actorId);

        runtime.Send(new NActors::IEventHandle(actorId, edgeId, new TCloudEvent(MakeCreateTopicEventInfo("/root/my/topic"))), 0, true);
        runtime.DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(events->size(), 1u);
        NJson::TJsonValue cloudEvent = ParseCloudEventProtobuf(events->front());
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.CreateTopic", "/root/my/topic");
    }

    Y_UNIT_TEST(DeleteTopicEventAudit) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        setup->GetServer().EnableLogs(
            {NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY},
            NActors::NLog::PRI_INFO
        );

        auto events = std::make_shared<TVector<TString>>();
        auto writer = MakeHolder<TInMemoryEventsWriter>(events);

        auto& runtime = setup->GetRuntime();
        auto edgeId = runtime.AllocateEdgeActor();
        auto actorId = runtime.Register(new TCloudEventsActor(std::move(writer)));
        runtime.EnableScheduleForActor(actorId);

        runtime.Send(new NActors::IEventHandle(actorId, edgeId, new TCloudEvent(MakeDeleteTopicEventInfo("/root/my/deleted_topic"))), 0, true);
        runtime.DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(events->size(), 1u);
        NJson::TJsonValue cloudEvent = ParseCloudEventProtobuf(events->front());
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.DeleteTopic", "/root/my/deleted_topic");
    }


    Y_UNIT_TEST(AlterTopicEventAudit) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        setup->GetServer().EnableLogs(
            {NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY},
            NActors::NLog::PRI_INFO
        );

        auto events = std::make_shared<TVector<TString>>();
        auto writer = MakeHolder<TInMemoryEventsWriter>(events);

        auto& runtime = setup->GetRuntime();
        auto edgeId = runtime.AllocateEdgeActor();
        auto actorId = runtime.Register(new TCloudEventsActor(std::move(writer)));
        runtime.EnableScheduleForActor(actorId);

        runtime.Send(new NActors::IEventHandle(actorId, edgeId, new TCloudEvent(MakeAlterTopicEventInfo())), 0, true);
        runtime.DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(events->size(), 1u);
        NJson::TJsonValue cloudEvent = ParseCloudEventProtobuf(events->front());
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.AlterTopic", "/root/db/topic1");

        const auto* requestParams = cloudEvent.GetValueByPath("request_parameters");
        UNIT_ASSERT_C(requestParams != nullptr, "Missing request_parameters");
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["path"].GetString(), "/root/db/topic1");
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["partition_write_speed_bytes_per_second"].GetString(), "123456");
        UNIT_ASSERT_VALUES_EQUAL((*requestParams)["consumers"].GetArraySafe().size(), 1u);
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["consumers"][0]["name"].GetString(), "consumer-1");
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["partitioning_settings"]["min_active_partitions"].GetString(), "2");
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["partitioning_settings"]["max_active_partitions"].GetString(), "5");

        const auto* details = cloudEvent.GetValueByPath("details");
        UNIT_ASSERT_C(details != nullptr, "Missing details");
        UNIT_ASSERT_STRINGS_EQUAL((*details)["partition_write_speed_bytes_per_second"].GetString(), "123456");
        UNIT_ASSERT_VALUES_EQUAL((*details)["consumers"].GetArraySafe().size(), 1u);
        UNIT_ASSERT_STRINGS_EQUAL((*details)["consumers"][0]["name"].GetString(), "consumer-1");
        UNIT_ASSERT_STRINGS_EQUAL((*details)["partitioning_settings"]["min_active_partitions"].GetString(), "2");
        UNIT_ASSERT_STRINGS_EQUAL((*details)["partitioning_settings"]["max_active_partitions"].GetString(), "5");

        UNIT_ASSERT(cloudEvent.GetMap().find("event_status") != cloudEvent.GetMap().end());
        UNIT_ASSERT_STRINGS_EQUAL(cloudEvent["event_status"].GetString(), "DONE");
    }

    Y_UNIT_TEST(CloudEventJsonFormat) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        setup->GetServer().EnableLogs(
            {NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY},
            NActors::NLog::PRI_INFO
        );

        auto events = std::make_shared<TVector<TString>>();
        auto writer = MakeHolder<TInMemoryEventsWriter>(events);

        auto& runtime = setup->GetRuntime();
        auto edgeId = runtime.AllocateEdgeActor();
        auto actorId = runtime.Register(new TCloudEventsActor(std::move(writer)));
        runtime.EnableScheduleForActor(actorId);

        runtime.Send(new NActors::IEventHandle(actorId, edgeId, new TCloudEvent(MakeCreateTopicEventInfo())), 0, true);
        runtime.DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(events->size(), 1u);
        NJson::TJsonValue cloudEvent = ParseCloudEventProtobuf(events->front());
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.CreateTopic", "/root/db/topic1");

        const auto* requestParams = cloudEvent.GetValueByPath("request_parameters");
        UNIT_ASSERT_C(requestParams != nullptr, "Missing request_parameters");
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["path"].GetString(), "/root/db/topic1");

        UNIT_ASSERT(cloudEvent.GetMap().find("event_status") != cloudEvent.GetMap().end());
        UNIT_ASSERT_STRINGS_EQUAL(cloudEvent["event_status"].GetString(), "DONE");
    }
}

Y_UNIT_TEST_SUITE(CloudEventsUaWriterTest) {
    Y_UNIT_TEST(WriteDelegatesToSession) {
        auto state = std::make_shared<TFakeUaEventsSession::TState>();
        TUaEventsWriter writer{MakeHolder<TFakeUaEventsSession>(state)};

        writer.Write("payload");

        UNIT_ASSERT_VALUES_EQUAL(state->Sent.size(), 1u);
        UNIT_ASSERT_STRINGS_EQUAL(state->Sent.front(), "payload");
        UNIT_ASSERT_VALUES_EQUAL(state->CloseCalls, 0u);
    }

    Y_UNIT_TEST(CloseIsIdempotent) {
        auto state = std::make_shared<TFakeUaEventsSession::TState>();
        TUaEventsWriter writer{MakeHolder<TFakeUaEventsSession>(state)};

        writer.Close();
        writer.Close();

        UNIT_ASSERT_VALUES_EQUAL(state->CloseCalls, 1u);
    }

    Y_UNIT_TEST(DestructorClosesSessionOnce) {
        auto state = std::make_shared<TFakeUaEventsSession::TState>();
        {
            TUaEventsWriter writer{MakeHolder<TFakeUaEventsSession>(state)};
            writer.Write("payload");
        }

        UNIT_ASSERT_VALUES_EQUAL(state->Sent.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(state->CloseCalls, 1u);
    }
}

} // namespace NKikimr::NPQ::NCloudEvents
