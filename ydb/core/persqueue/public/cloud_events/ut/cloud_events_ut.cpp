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
    void Write(const TString& data) override {
        Events.push_back(data);
    }

    const TVector<TString>& GetEvents() const {
        return Events;
    }

private:
    TVector<TString> Events;
};

Y_UNIT_TEST_SUITE(CloudEventsAuditTest) {
    Y_UNIT_TEST(CreateTopicEventAudit) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        setup->GetServer().EnableLogs(
            {NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY},
            NActors::NLog::PRI_INFO
        );

        auto writer = MakeHolder<TInMemoryEventsWriter>();
        auto* writerPtr = writer.Get();

        auto& runtime = setup->GetRuntime();
        auto edgeId = runtime.AllocateEdgeActor();
        auto actorId = runtime.Register(new TCloudEventsActor(std::move(writer)));
        runtime.EnableScheduleForActor(actorId);

        runtime.Send(new NActors::IEventHandle(actorId, edgeId, new TCloudEvent(MakeCreateTopicEventInfo("/root/my/topic"))), 0, true);
        runtime.DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(writerPtr->GetEvents().size(), 1u);
        NJson::TJsonValue cloudEvent = ParseCloudEventProtobuf(writerPtr->GetEvents().front());
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.CreateTopic", "/root/my/topic");
    }

    Y_UNIT_TEST(DeleteTopicEventAudit) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        setup->GetServer().EnableLogs(
            {NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY},
            NActors::NLog::PRI_INFO
        );

        auto writer = MakeHolder<TInMemoryEventsWriter>();
        auto* writerPtr = writer.Get();

        auto& runtime = setup->GetRuntime();
        auto edgeId = runtime.AllocateEdgeActor();
        auto actorId = runtime.Register(new TCloudEventsActor(std::move(writer)));
        runtime.EnableScheduleForActor(actorId);

        runtime.Send(new NActors::IEventHandle(actorId, edgeId, new TCloudEvent(MakeDeleteTopicEventInfo("/root/my/deleted_topic"))), 0, true);
        runtime.DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(writerPtr->GetEvents().size(), 1u);
        NJson::TJsonValue cloudEvent = ParseCloudEventProtobuf(writerPtr->GetEvents().front());
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.DeleteTopic", "/root/my/deleted_topic");
    }

    Y_UNIT_TEST(CloudEventJsonFormat) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false);
        setup->GetServer().EnableLogs(
            {NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY},
            NActors::NLog::PRI_INFO
        );

        auto writer = MakeHolder<TInMemoryEventsWriter>();
        auto* writerPtr = writer.Get();

        auto& runtime = setup->GetRuntime();
        auto edgeId = runtime.AllocateEdgeActor();
        auto actorId = runtime.Register(new TCloudEventsActor(std::move(writer)));
        runtime.EnableScheduleForActor(actorId);

        runtime.Send(new NActors::IEventHandle(actorId, edgeId, new TCloudEvent(MakeCreateTopicEventInfo())), 0, true);
        runtime.DispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(writerPtr->GetEvents().size(), 1u);
        NJson::TJsonValue cloudEvent = ParseCloudEventProtobuf(writerPtr->GetEvents().front());
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.CreateTopic", "/root/db/topic1");

        const auto* requestParams = cloudEvent.GetValueByPath("request_parameters");
        UNIT_ASSERT_C(requestParams != nullptr, "Missing request_parameters");
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["path"].GetString(), "/root/db/topic1");

        UNIT_ASSERT(cloudEvent.GetMap().find("event_status") != cloudEvent.GetMap().end());
        UNIT_ASSERT_STRINGS_EQUAL(cloudEvent["event_status"].GetString(), "DONE");
    }
}

} // namespace NKikimr::NPQ::NCloudEvents
