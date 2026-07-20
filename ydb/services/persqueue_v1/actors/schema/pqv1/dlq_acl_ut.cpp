#include "actors.h"

#include <ydb/core/testlib/grpc_request/grpc_request.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

using namespace NYdb::NTopic::NTests;
using namespace NKikimr::NPersQueueTests;
using namespace NKikimr::Tests::NGrpc;

namespace {

constexpr const char* USER_PASSWORD = "password";
constexpr const char* MLP_CONSUMER_NAME = "mlp-consumer";
constexpr const char* DATABASE = "/Root";

Ydb::PersQueue::V1::TopicSettings::ReadRule MakeReadRuleWithDlq(
    const TString& consumerName,
    const TString& dlqTopic
) {
    Ydb::PersQueue::V1::TopicSettings::ReadRule readRule;
    readRule.set_consumer_name(consumerName);
    readRule.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    readRule.set_version(1);

    auto& shared = *readRule.mutable_shared_consumer_type();
    shared.set_keep_messages_order(true);
    auto& deadLetterPolicy = *shared.mutable_dead_letter_policy();
    deadLetterPolicy.set_enabled(true);
    deadLetterPolicy.mutable_condition()->set_max_processing_attempts(5);
    if (!dlqTopic.empty()) {
        deadLetterPolicy.mutable_move_action()->set_dead_letter_queue(dlqTopic);
    }

    return readRule;
}

void FillDefaultTopicSettings(Ydb::PersQueue::V1::TopicSettings& settings) {
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Hours(24).MilliSeconds());
}

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>(
        "DlqAclPQv1",
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

template<typename TRequest, typename TResponse>
std::shared_ptr<TResultHolder<TResponse>> DoRequest(
    NActors::TTestActorRuntime& runtime,
    const TRequest& request,
    NActors::IActor* (*createActor)(NGRpcService::IRequestOpCtx*),
    const TString& path,
    const TString& database = DATABASE,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr
) {
    auto result = std::make_shared<TResultHolder<TResponse>>();
    auto edgeActor = runtime.AllocateEdgeActor();

    auto ctx = new TRequestCtx<TRequest, TResponse>(
        request,
        path,
        database,
        result,
        edgeActor,
        std::move(userToken)
    );
    runtime.Register(createActor(ctx));

    runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edgeActor, TDuration::Seconds(10));

    UNIT_ASSERT_C(result->ResultStatus, "The operation is still in progress");
    return result;
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

TString MakeTopicPath(const char* topicName) {
    return TStringBuilder() << DATABASE << "/" << topicName;
}

Ydb::PersQueue::V1::CreateTopicRequest MakeCreateTopicRequest(
    const TString& path,
    const TString& consumerName = {},
    const TString& dlqTopic = {}
) {
    Ydb::PersQueue::V1::CreateTopicRequest request;
    request.set_path(path);
    FillDefaultTopicSettings(*request.mutable_settings());

    if (!consumerName.empty()) {
        *request.mutable_settings()->add_read_rules() = MakeReadRuleWithDlq(consumerName, dlqTopic);
    }

    return request;
}

void CreateDLQTopic(const std::shared_ptr<TTopicSdkTestSetup>& setup, const char* dlqTopic) {
    auto& runtime = setup->GetRuntime();
    const TString dlqTopicPath = MakeTopicPath(dlqTopic);
    auto request = MakeCreateTopicRequest(dlqTopicPath);
    auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime,
        request,
        CreateCreateTopicActor,
        dlqTopicPath
    );
    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    setup->GetServer().WaitInit(dlqTopicPath);
}

std::shared_ptr<TResultHolder<Ydb::PersQueue::V1::CreateTopicResponse>> CreateTopicWithDLQ(
    const std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& userSid,
    const char* mainTopic,
    const char* dlqTopic
) {
    auto& runtime = setup->GetRuntime();
    const auto userToken = MakeUserToken(userSid);
    const TString mainTopicPath = MakeTopicPath(mainTopic);
    auto request = MakeCreateTopicRequest(mainTopicPath, MLP_CONSUMER_NAME, dlqTopic);
    return DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
        runtime,
        request,
        CreateCreateTopicActor,
        mainTopicPath,
        DATABASE,
        userToken
    );
}

std::shared_ptr<TResultHolder<Ydb::PersQueue::V1::AddReadRuleResponse>> AddReadRuleWithDLQ(
    const std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& userSid,
    const char* mainTopic,
    const char* dlqTopic
) {
    auto& runtime = setup->GetRuntime();
    const auto userToken = MakeUserToken(userSid);
    const TString mainTopicPath = MakeTopicPath(mainTopic);

    Ydb::PersQueue::V1::AddReadRuleRequest request;
    request.set_path(mainTopicPath);
    *request.mutable_read_rule() = MakeReadRuleWithDlq(MLP_CONSUMER_NAME, dlqTopic);

    return DoRequest<Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
        runtime,
        request,
        CreateAddConsumerActor,
        mainTopicPath,
        DATABASE,
        userToken
    );
}

std::shared_ptr<TResultHolder<Ydb::PersQueue::V1::AddReadRuleResponse>> CreateAndAlterTopicWithDLQ(
    const std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& userSid,
    const char* mainTopic,
    const char* dlqTopic
) {
    auto& runtime = setup->GetRuntime();
    const auto userToken = MakeUserToken(userSid);
    const TString mainTopicPath = MakeTopicPath(mainTopic);

    {
        auto request = MakeCreateTopicRequest(mainTopicPath);
        auto result = DoRequest<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
            runtime,
            request,
            CreateCreateTopicActor,
            mainTopicPath,
            DATABASE,
            userToken
        );
        UNIT_ASSERT(result->ResultStatus);
        UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
        setup->GetServer().WaitInit(mainTopicPath);
    }

    return AddReadRuleWithDLQ(setup, userSid, mainTopic, dlqTopic);
}

std::shared_ptr<TResultHolder<Ydb::PersQueue::V1::AlterTopicResponse>> AlterTopicMlpConsumerDlq(
    const std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& userSid,
    const char* mainTopic,
    const char* newDlqTopic
) {
    auto& runtime = setup->GetRuntime();
    const auto userToken = MakeUserToken(userSid);
    const TString mainTopicPath = MakeTopicPath(mainTopic);

    Ydb::PersQueue::V1::AlterTopicRequest request;
    request.set_path(mainTopicPath);
    FillDefaultTopicSettings(*request.mutable_settings());
    *request.mutable_settings()->add_read_rules() = MakeReadRuleWithDlq(MLP_CONSUMER_NAME, newDlqTopic);

    return DoRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
        runtime,
        request,
        CreateAlterTopicActor,
        mainTopicPath,
        DATABASE,
        userToken
    );
}

std::shared_ptr<TResultHolder<Ydb::PersQueue::V1::AlterTopicResponse>> AlterTopicRetention(
    const std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& userSid,
    const char* mainTopic,
    const char* dlqTopic
) {
    auto& runtime = setup->GetRuntime();
    const auto userToken = MakeUserToken(userSid);
    const TString mainTopicPath = MakeTopicPath(mainTopic);

    Ydb::PersQueue::V1::AlterTopicRequest request;
    request.set_path(mainTopicPath);
    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Hours(48).MilliSeconds());
    *settings.add_read_rules() = MakeReadRuleWithDlq(MLP_CONSUMER_NAME, dlqTopic);

    return DoRequest<Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
        runtime,
        request,
        CreateAlterTopicActor,
        mainTopicPath,
        DATABASE,
        userToken
    );
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

Y_UNIT_TEST_SUITE(DlqAclPQv1) {

Y_UNIT_TEST(CreateTopicWithMlpConsumerFailsWithoutDlqTopicAccess) {
    constexpr const char* USER_NAME = "pqv1topicuser";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-acl-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-acl-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    RevokeAllRightsOnTopic(client, DLQ_TOPIC, userSid);

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::UNAUTHORIZED, result->Issues.ToString());
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerSucceedsWithOnlyAlterSchemaOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser2";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-alterschema-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-alterschema-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::AlterSchema});

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerSucceedsWithOnlyUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser3";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-updaterow-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-updaterow-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::UpdateRow});

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerSucceedsWithAlterSchemaAndUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser4";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-both-rights-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-both-rights-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {
        NACLib::EAccessRights::AlterSchema,
        NACLib::EAccessRights::UpdateRow,
    });

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerFailsWhenDlqTopicDoesNotExist) {
    constexpr const char* USER_NAME = "pqv1topicuser6";
    constexpr const char* DLQ_TOPIC = "pqv1-nonexistent-dlq-topic";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-missing-dlq-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SCHEME_ERROR, result->Issues.ToString());
}

Y_UNIT_TEST(CreateTopicWithMlpConsumerFailsWithOnlySelectRowOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser5";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-read-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-read-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::SelectRow});

    auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::UNAUTHORIZED, result->Issues.ToString());
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerFailsWithoutDlqTopicAccess) {
    constexpr const char* USER_NAME = "pqv1topicuser";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-acl-alter-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-acl-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    RevokeAllRightsOnTopic(client, DLQ_TOPIC, userSid);

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::UNAUTHORIZED, result->Issues.ToString());
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerSucceedsWithOnlyAlterSchemaOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser2";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-alterschema-alter-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-alterschema-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::AlterSchema});

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerSucceedsWithOnlyUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser3";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-updaterow-alter-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-updaterow-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::UpdateRow});

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerSucceedsWithAlterSchemaAndUpdateRowOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser4";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-both-rights-alter-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-both-rights-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {
        NACLib::EAccessRights::AlterSchema,
        NACLib::EAccessRights::UpdateRow,
    });

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
    setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
}

Y_UNIT_TEST(AlterTopicUnrelatedChangeSucceedsAfterDlqAccessRevoked) {
    constexpr const char* USER_NAME = "pqv1topicuser7";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-unrelated-alter-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-unrelated-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {
        NACLib::EAccessRights::AlterSchema,
        NACLib::EAccessRights::UpdateRow,
    });

    {
        auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);
        UNIT_ASSERT(result->ResultStatus);
        UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
        setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
    }

    RevokeAllRightsOnTopic(client, DLQ_TOPIC, userSid);

    auto result = AlterTopicRetention(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerFailsWhenDlqTopicDoesNotExist) {
    constexpr const char* USER_NAME = "pqv1topicuser6";
    constexpr const char* DLQ_TOPIC = "pqv1-nonexistent-dlq-alter-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-missing-dlq-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SCHEME_ERROR, result->Issues.ToString());
}

Y_UNIT_TEST(AlterTopicMlpConsumerFailsWhenChangingDlqToTopicWithoutAccess) {
    constexpr const char* USER_NAME = "pqv1topicuser8";
    constexpr const char* DLQ_TOPIC_1 = "pqv1-dlq-topic-switch-1";
    constexpr const char* DLQ_TOPIC_2 = "pqv1-dlq-topic-switch-2";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-dlq-switch-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC_1);
    GrantOnlyOnTopic(client, DLQ_TOPIC_1, userSid, {
        NACLib::EAccessRights::AlterSchema,
        NACLib::EAccessRights::UpdateRow,
    });

    CreateDLQTopic(setup, DLQ_TOPIC_2);
    RevokeAllRightsOnTopic(client, DLQ_TOPIC_2, userSid);

    {
        auto result = CreateTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC_1);
        UNIT_ASSERT(result->ResultStatus);
        UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::SUCCESS, result->Issues.ToString());
        setup->GetServer().WaitInit(MakeTopicPath(MAIN_TOPIC));
    }

    auto result = AlterTopicMlpConsumerDlq(setup, userSid, MAIN_TOPIC, DLQ_TOPIC_2);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::UNAUTHORIZED, result->Issues.ToString());
}

Y_UNIT_TEST(AlterTopicWithMlpConsumerFailsWithOnlySelectRowOnDlqTopic) {
    constexpr const char* USER_NAME = "pqv1topicuser5";
    constexpr const char* DLQ_TOPIC = "pqv1-dlq-topic-read-alter-test";
    constexpr const char* MAIN_TOPIC = "pqv1-main-topic-read-alter-test";

    auto setup = CreateSetup();
    auto& client = *setup->GetServer().AnnoyingClient;

    const TString userSid = CreateUser(client, USER_NAME);

    CreateDLQTopic(setup, DLQ_TOPIC);

    GrantOnlyOnTopic(client, DLQ_TOPIC, userSid, {NACLib::EAccessRights::SelectRow});

    auto result = CreateAndAlterTopicWithDLQ(setup, userSid, MAIN_TOPIC, DLQ_TOPIC);

    UNIT_ASSERT(result->ResultStatus);
    UNIT_ASSERT_VALUES_EQUAL_C(*result->ResultStatus, Ydb::StatusIds::UNAUTHORIZED, result->Issues.ToString());
}

} // Y_UNIT_TEST_SUITE(DlqAclPQv1)

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
