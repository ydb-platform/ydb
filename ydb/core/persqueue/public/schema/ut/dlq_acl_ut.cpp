#include <ydb/core/persqueue/public/schema/schema.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NPQ::NSchema {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::NPersQueueTests;
using namespace NKikimr::Tests;

namespace {

constexpr const char* USER_PASSWORD = "password";
constexpr const char* MLP_CONSUMER_NAME = "mlp-consumer";

void FillMlpConsumerWithDlq(
    Ydb::Topic::Consumer& consumer,
    const TString& consumerName,
    const TString& dlqTopic
) {
    consumer.set_name(consumerName);
    auto* shared = consumer.mutable_shared_consumer_type();
    auto* deadLetterPolicy = shared->mutable_dead_letter_policy();
    deadLetterPolicy->set_enabled(true);
    deadLetterPolicy->mutable_condition()->set_max_processing_attempts(5);
    if (!dlqTopic.empty()) {
        deadLetterPolicy->mutable_move_action()->set_dead_letter_queue(dlqTopic);
    }
}

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>(
        "DlqAcl",
        TTopicSdkTestSetup::MakeServerSettings(),
        false
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_SCHEMA,
            NKikimrServices::TX_PROXY,
            NKikimrServices::TX_PROXY_SCHEME_CACHE,
        },
        NActors::NLog::PRI_DEBUG
    );
    return setup;
}

Ydb::Topic::CreateTopicRequest MakeCreateTopicRequest(
    const TString& path,
    const TString& consumerName = {},
    const TString& dlqTopic = {}
) {
    Ydb::Topic::CreateTopicRequest request;
    request.set_path(path);
    request.mutable_retention_period()->set_seconds(TDuration::Hours(24).Seconds());

    auto* partitioning = request.mutable_partitioning_settings();
    partitioning->set_min_active_partitions(1);

    if (!consumerName.empty()) {
        FillMlpConsumerWithDlq(*request.add_consumers(), consumerName, dlqTopic);
    }

    return request;
}

THolder<TEvSchemaResponse> DoAlterRequest(
    NActors::TTestActorRuntime& runtime,
    const Ydb::Topic::AlterTopicRequest& request,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
) {
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateAlterTopicActor(edge, {
        .Database = "/Root",
        .Request = request,
        .UserToken = std::move(userToken),
        .IfExists = false,
        .PrepareOnly = false,
        .Cookie = 0,
    }));

    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(10));
}

THolder<TEvSchemaResponse> DoRequest(
    NActors::TTestActorRuntime& runtime,
    const Ydb::Topic::CreateTopicRequest& request,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
) {
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(CreateCreateTopicActor(edge, {
        .Database = "/Root",
        .Request = request,
        .UserToken = std::move(userToken),
        .IfNotExists = false,
        .PrepareOnly = false,
        .Cookie = 0,
    }));

    return runtime.GrabEdgeEvent<TEvSchemaResponse>(TDuration::Seconds(10));
}

TIntrusiveConstPtr<NACLib::TUserToken> MakeUserToken(const TString& userSid) {
    auto token = MakeIntrusive<NACLib::TUserToken>(userSid, TVector<NACLib::TSID>{});
    token->SaveSerializationInfo();
    return token;
}

TString CreateUser(TFlatMsgBusPQClient& client, const char* userName) {
    const TString userSid = TString(userName) + "@" BUILTIN_ACL_DOMAIN;
    client.TestCreateUser("/Root", userName, USER_PASSWORD, "root@builtin");
    client.TestGrantConnect(userSid);
    client.TestGrant("/", "Root", userSid, NACLib::EAccessRights::CreateQueue);
    return userSid;
}

void CreateDLQTopic(const std::shared_ptr<TTopicSdkTestSetup>& setup, const char* dlqTopic) {
    auto& runtime = setup->GetRuntime();
    const TString dlqTopicPath = TStringBuilder() << "/Root/" << dlqTopic;
    auto request = MakeCreateTopicRequest(dlqTopicPath);
    auto result = DoRequest(runtime, request);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
    setup->GetServer().WaitInit(dlqTopicPath);
}

THolder<TEvSchemaResponse> CreateTopicWithDLQ(
    const std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& userSid,
    const char* mainTopic,
    const char* dlqTopic
) {
    auto& runtime = setup->GetRuntime();
    const auto userToken = MakeUserToken(userSid);
    const TString mainTopicPath = TStringBuilder() << "/Root/" << mainTopic;
    auto request = MakeCreateTopicRequest(mainTopicPath, MLP_CONSUMER_NAME, dlqTopic);
    return DoRequest(runtime, request, userToken);
}

THolder<TEvSchemaResponse> CreateAndAlterTopicWithDLQ(
    const std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& userSid,
    const char* mainTopic,
    const char* dlqTopic
) {
    auto& runtime = setup->GetRuntime();
    const auto userToken = MakeUserToken(userSid);
    const TString mainTopicPath = TStringBuilder() << "/Root/" << mainTopic;

    {
        auto request = MakeCreateTopicRequest(mainTopicPath);
        auto result = DoRequest(runtime, request, userToken);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
        setup->GetServer().WaitInit(mainTopicPath);
    }

    Ydb::Topic::AlterTopicRequest request;
    request.set_path(mainTopicPath);
    FillMlpConsumerWithDlq(*request.add_add_consumers(), MLP_CONSUMER_NAME, dlqTopic);
    return DoAlterRequest(runtime, request, userToken);
}

void RevokeAllRightsOnTopic(TFlatMsgBusPQClient& client, const TString& topicName, const TString& userSid) {
    NACLib::TDiffACL diffAcl;
    diffAcl.SetInterruptInheritance(true);
    diffAcl.AddAccess(
        NACLib::EAccessType::Deny,
        NACLib::EAccessRights::AlterSchema | NACLib::EAccessRights::UpdateRow,
        userSid
    );
    client.TestModifyACL("/Root", topicName, diffAcl.SerializeAsString());
}

void GrantOnlyOnTopic(
    TFlatMsgBusPQClient& client,
    const TString& topicName,
    const TString& userSid,
    std::initializer_list<NACLib::EAccessRights> permissions
) {
    NACLib::TDiffACL diffAcl;
    diffAcl.SetInterruptInheritance(true);
    for (const auto permission : permissions) {
        diffAcl.AddAccess(NACLib::EAccessType::Allow, permission, userSid);
    }
    client.TestModifyACL("/Root", topicName, diffAcl.SerializeAsString());
}

} // namespace

Y_UNIT_TEST_SUITE(DlqAcl) {

Y_UNIT_TEST(CreateTopicWithMlpConsumerFailsWithoutDlqTopicAccess) {
    constexpr const char* USER_NAME = "topicuser";
    constexpr const char* DLQ_TOPIC = "dlq-topic-acl-test";
    constexpr const char* MAIN_TOPIC = "main-topic-acl-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    RevokeAllRightsOnTopic(client, DLQ_TOPIC, userSid);

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::UNAUTHORIZED, result->ErrorMessage);
    UNIT_ASSERT(!result->ErrorMessage.empty());
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerSucceedsWithOnlyAlterSchemaOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser2";
    constexpr const char* DLQ_TOPIC = "dlq-topic-alterschema-test";
    constexpr const char* MAIN_TOPIC = "main-topic-alterschema-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::AlterSchema});

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
    setup->GetServer().WaitInit(TStringBuilder() << "/Root/" << MAIN_TOPIC);
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerSucceedsWithOnlyUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser3";
    constexpr const char* DLQ_TOPIC = "dlq-topic-updaterow-test";
    constexpr const char* MAIN_TOPIC = "main-topic-updaterow-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::UpdateRow});

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
    setup->GetServer().WaitInit(TStringBuilder() << "/Root/" << MAIN_TOPIC);
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerSucceedsWithAlterSchemaAndUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser4";
    constexpr const char* DLQ_TOPIC = "dlq-topic-both-rights-test";
    constexpr const char* MAIN_TOPIC = "main-topic-both-rights-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {
        NACLib::EAccessRights::AlterSchema,
        NACLib::EAccessRights::UpdateRow,
    });

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
    setup->GetServer().WaitInit(TStringBuilder() << "/Root/" << MAIN_TOPIC);
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerFailsWithOnlySelectRowOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser5";
    constexpr const char* DLQ_TOPIC = "dlq-topic-read-test";
    constexpr const char* MAIN_TOPIC = "main-topic-read-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::SelectRow});

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::UNAUTHORIZED, result->ErrorMessage);
    UNIT_ASSERT(!result->ErrorMessage.empty());
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerFailsWithoutDlqTopicAccess) {
    constexpr const char* USER_NAME = "topicuser";
    constexpr const char* DLQ_TOPIC = "dlq-topic-acl-alter-test";
    constexpr const char* MAIN_TOPIC = "main-topic-acl-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    RevokeAllRightsOnTopic(client, DLQ_TOPIC, userSid);

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::UNAUTHORIZED, result->ErrorMessage);
    UNIT_ASSERT(!result->ErrorMessage.empty());
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerSucceedsWithOnlyAlterSchemaOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser2";
    constexpr const char* DLQ_TOPIC = "dlq-topic-alterschema-alter-test";
    constexpr const char* MAIN_TOPIC = "main-topic-alterschema-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::AlterSchema});

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
    setup->GetServer().WaitInit(TStringBuilder() << "/Root/" << MAIN_TOPIC);
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerSucceedsWithOnlyUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser3";
    constexpr const char* DLQ_TOPIC = "dlq-topic-updaterow-alter-test";
    constexpr const char* MAIN_TOPIC = "main-topic-updaterow-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::UpdateRow});

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
    setup->GetServer().WaitInit(TStringBuilder() << "/Root/" << MAIN_TOPIC);
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerSucceedsWithAlterSchemaAndUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser4";
    constexpr const char* DLQ_TOPIC = "dlq-topic-both-rights-alter-test";
    constexpr const char* MAIN_TOPIC = "main-topic-both-rights-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {
        NACLib::EAccessRights::AlterSchema,
        NACLib::EAccessRights::UpdateRow,
    });

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorMessage);
    setup->GetServer().WaitInit(TStringBuilder() << "/Root/" << MAIN_TOPIC);
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerFailsWithOnlySelectRowOnDlqTopic) {
    constexpr const char* USER_NAME = "topicuser5";
    constexpr const char* DLQ_TOPIC = "dlq-topic-read-alter-test";
    constexpr const char* MAIN_TOPIC = "main-topic-read-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::SelectRow});

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::UNAUTHORIZED, result->ErrorMessage);
    UNIT_ASSERT(!result->ErrorMessage.empty());
}

} // Y_UNIT_TEST_SUITE(DlqAcl)

} // namespace NKikimr::NPQ::NSchema
