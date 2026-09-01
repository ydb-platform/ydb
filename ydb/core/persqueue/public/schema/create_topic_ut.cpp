#include "schema_ut_helpers.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NSchema {

using namespace NTests;

Y_UNIT_TEST_SUITE(CreateTopic) {

Y_UNIT_TEST(CreateTopicWithNameEqDB) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    Ydb::Topic::CreateTopicRequest request;
    request.set_path("/Root");

    AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(CreateTopicSuccess) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_create_ok";

    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path)), Ydb::StatusIds::SUCCESS);

    auto config = DescribeTabletConfig(runtime, path);
    const auto* consumer = NPQ::GetConsumer(config, "user");
    UNIT_ASSERT(consumer);
    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(consumer->GetType()),
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING));
}

Y_UNIT_TEST(CreateTopicKeepsLiteralDashDashName) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    const TString name = "TestSchemeList--test-topic-1";
    const TString path = "/Root/" + name;
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(name)), Ydb::StatusIds::SUCCESS);

    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(NDescriber::CreateDescriberActor(edge, "/Root", {name}));
    auto response = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), 1u);
    const auto it = response->Topics.find(name);
    UNIT_ASSERT(it != response->Topics.end());
    UNIT_ASSERT_VALUES_EQUAL(it->second.Status, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(it->second.RealPath, path);

    // "--" is not a path separator: the converted legacy path must not exist.
    auto convertedEdge = runtime.AllocateEdgeActor();
    runtime.Register(NDescriber::CreateDescriberActor(convertedEdge, "/Root", {TString("TestSchemeList/test-topic-1")}));
    auto converted = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(converted->Topics.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(converted->Topics.begin()->second.Status, NDescriber::EStatus::NOT_FOUND);
}

// https://github.com/ydb-platform/ydb/issues/50971
// Go SDK TestSchemeList creates a topic named like t.Name() (contains "--") and
// lists the database root. FCC must keep the leaf literal: no -- → / rewrite.
Y_UNIT_TEST(CreateFccLegacyLookingNameListedAsLiteral) {
    auto setup = CreateSetup("FccLegacyLookingSchemeList");
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    const TVector<TString> names = {
        "TestSchemeList--test-topic-1",
        "rt3.dc1--account--topic",
    };
    for (const auto& name : names) {
        AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(name)), Ydb::StatusIds::SUCCESS);
    }

    auto ls = setup->GetServer().AnnoyingClient->Ls("/Root");
    UNIT_ASSERT(ls);
    UNIT_ASSERT_VALUES_EQUAL(ls->Record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);

    auto findChild = [&](TStringBuf childName) -> const NKikimrSchemeOp::TDirEntry* {
        for (const auto& child : ls->Record.GetPathDescription().GetChildren()) {
            if (child.GetName() == childName) {
                return &child;
            }
        }
        return nullptr;
    };

    for (const auto& name : names) {
        const auto* child = findChild(name);
        UNIT_ASSERT_C(child, name);
        UNIT_ASSERT_VALUES_EQUAL(child->GetPathType(), NKikimrSchemeOp::EPathTypePersQueueGroup);
    }
    UNIT_ASSERT(!findChild("TestSchemeList"));
    UNIT_ASSERT(!findChild("account"));
}

Y_UNIT_TEST(CreateSharedConsumer) {
    auto setup = CreateSetup("CoreCreateShared");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_shared";
    const TString dlqPath = "/Root/dlq";

    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(dlqPath)), Ydb::StatusIds::SUCCESS);

    auto request = MakeCreateTopicRequest(path);
    request.clear_consumers();
    auto* consumer = request.add_consumers();
    consumer->set_name("shared_c1");
    auto* type = consumer->mutable_shared_consumer_type();
    type->set_keep_messages_order(true);
    type->mutable_default_processing_timeout()->set_seconds(3);
    type->mutable_dead_letter_policy()->set_enabled(true);
    type->mutable_dead_letter_policy()->mutable_condition()->set_max_processing_attempts(11);
    type->mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue("dlq");

    AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);

    auto config = DescribeTabletConfig(runtime, path);
    const auto* c = NPQ::GetConsumer(config, "shared_c1");
    UNIT_ASSERT(c);
    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(c->GetType()),
        NKikimrPQ::TPQTabletConfig::EConsumerType_Name(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP));
    UNIT_ASSERT(c->GetKeepMessageOrder());
    UNIT_ASSERT_VALUES_EQUAL(c->GetDefaultProcessingTimeoutSeconds(), 3);
    UNIT_ASSERT_VALUES_EQUAL(c->GetMaxProcessingAttempts(), 11);
    UNIT_ASSERT_VALUES_EQUAL(c->GetDeadLetterQueue(), "dlq");
}

Y_UNIT_TEST(CreateSharedConsumerEmptyDlqRejected) {
    auto setup = CreateSetup("CoreCreateSharedEmptyDlq");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_shared_empty_dlq";

    auto request = MakeCreateTopicRequest(path);
    request.clear_consumers();
    auto* consumer = request.add_consumers();
    consumer->set_name("shared_c1");
    auto* type = consumer->mutable_shared_consumer_type();
    type->mutable_dead_letter_policy()->set_enabled(true);
    type->mutable_dead_letter_policy()->mutable_move_action();

    AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, "Dead letter queue cannot be empty");
}

Y_UNIT_TEST(SharedConsumersDisabledRejected) {
    auto setup = CreateSetup("CoreSharedDisabled");
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicMessageLevelParallelism(false);

    auto request = MakeCreateTopicRequest("/Root/topic_shared_off");
    request.clear_consumers();
    auto* consumer = request.add_consumers();
    consumer->set_name("shared_c1");
    consumer->mutable_shared_consumer_type();

    AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, "shared consumers are disabled");
}

Y_UNIT_TEST(CreateMessagesPerSecond) {
    auto setup = CreateSetup("CoreCreateMsgPerSec");
    auto& runtime = setup->GetRuntime();

    {
        const TString path = "/Root/topic_msg_per_sec";
        auto request = MakeCreateTopicRequest(path);
        request.set_partition_write_speed_messages_per_second(777);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);

        const auto& partConfig = DescribePartitionConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(partConfig.GetWriteSpeedInMessagesPerSecond(), 777u);
        UNIT_ASSERT_VALUES_EQUAL(partConfig.GetBurstSizeInMessages(), 777u);
    }

    {
        const TString path = "/Root/topic_msg_default";
        AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path)), Ydb::StatusIds::SUCCESS);

        const auto& partConfig = DescribePartitionConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(
            partConfig.GetWriteSpeedInMessagesPerSecond(),
            DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND);
        UNIT_ASSERT_VALUES_EQUAL(
            partConfig.GetBurstSizeInMessages(),
            DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND);
    }

    {
        auto request = MakeCreateTopicRequest("/Root/topic_msg_neg");
        request.set_partition_write_speed_messages_per_second(-1);
        AssertStatus(
            DoCreate(runtime, request),
            Ydb::StatusIds::BAD_REQUEST,
            "partition_write_speed_messages_per_second");
    }
}

Y_UNIT_TEST(CreateTopicWithIdAttribute) {
    auto setup = CreateSetup("CoreCreateIdAttr");
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);

    // FirstClass: the _id attribute is silently ignored, the Id is the topic's
    // LocalPathId set by schemeshard, never taken from the request.
    {
        const TString path = "/Root/topic_id_attr_first_class";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "1234567";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        // The stored Id is the LocalPathId, not the supplied _id value.
        UNIT_ASSERT_VALUES_UNEQUAL(config.GetId().GetId(), 1234567u);
    }

    // Flag off: the attribute is ignored as well.
    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(false);
        const TString path = "/Root/topic_id_attr_flag_off";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "1234567";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT(!config.HasId());
    }

    // Federation + flag on: the _id attribute is accepted as the topic Id.
    // IdTxStep is stamped 0 (sentinel = "filled at create") so writers never
    // enable the name-keyed fallback for a brand-new federation topic.
    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        const TString path = "/Root/rt3.dc1--test_account--topic_id_attr_federation";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "1234567";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(config.GetDC(), "dc1");
        UNIT_ASSERT_VALUES_EQUAL(config.GetProducer(), "test_account");
        UNIT_ASSERT_VALUES_EQUAL(config.GetTopic(), "topic_id_attr_federation");
        UNIT_ASSERT_VALUES_EQUAL(config.GetId().GetId(), 1234567u);
        UNIT_ASSERT_VALUES_EQUAL(config.GetId().GetOwnerId(), 0u);
        UNIT_ASSERT(config.GetId().HasTxStep());
        UNIT_ASSERT_VALUES_EQUAL(config.GetId().GetTxStep(), 0u); // sentinel: filled at create
        // Restore FirstClass mode for subsequent sub-cases if any.
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    }

    // Non-numeric _id must be rejected with BAD_REQUEST when flag is on and federation mode.
    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        const TString path = "/Root/rt3.dc1--test_account--topic_bad_id";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "not-a-number";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, "not a valid positive integer");
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    }

    // _id = "0" must be rejected with BAD_REQUEST when flag is on and federation mode.
    {
        runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        const TString path = "/Root/rt3.dc1--test_account--topic_zero_id";
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_id"] = "0";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, "must be greater than 0");
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    }
}

} // Y_UNIT_TEST_SUITE(CreateTopic)

} // namespace NKikimr::NPQ::NSchema
