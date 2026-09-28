#include "check_dlq_topics.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NSchema {

using namespace NYdb::NTopic::NTests;
using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

void EnableLogs(TTopicSdkTestSetup& setup) {
    setup.GetServer().EnableLogs(
        {NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER, NKikimrServices::PQ_SCHEMA},
        NActors::NLog::PRI_DEBUG
    );
}

void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
    TDriver driver(setup.MakeDriverConfig());
    TQueryClient client(driver);
    auto session = client.GetSession().GetValueSync().GetSession();
    auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    driver.Stop(true);
}

void ModifyTopicAcl(TTopicSdkTestSetup& setup, const TString& topicName, const NACLib::TDiffACL& acl) {
    setup.GetServer().AnnoyingClient->ModifyACL("/Root", topicName, acl.SerializeAsString());
}

TIntrusiveConstPtr<NACLib::TUserToken> MakeUserToken(const TString& userSid) {
    auto token = MakeIntrusive<NACLib::TUserToken>(userSid, TVector<NACLib::TSID>{});
    token->SaveSerializationInfo();
    return token;
}

THolder<TEvCheckDlqTopicsResponse> CheckDlq(
    NActors::TTestActorRuntime& runtime,
    const NKikimrPQ::TPQTabletConfig& newConfig,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr,
    const TString& databasePath = "/Root",
    const NKikimrPQ::TPQTabletConfig& oldConfig = {})
{
    auto edge = runtime.AllocateEdgeActor();
    auto* actor = CreateCheckDlqTopicsActorIfNeeded(
        edge,
        databasePath,
        newConfig,
        oldConfig,
        TCheckDlqTopicsSettings{.UserToken = std::move(userToken)}
    );
    if (!actor) {
        return MakeHolder<TEvCheckDlqTopicsResponse>(Ydb::StatusIds::SUCCESS);
    }
    auto actorId = runtime.Register(actor);
    runtime.EnableScheduleForActor(actorId);
    return runtime.GrabEdgeEvent<TEvCheckDlqTopicsResponse>(TDuration::Seconds(10));
}

void AssertStatus(
    const THolder<TEvCheckDlqTopicsResponse>& result,
    Ydb::StatusIds::StatusCode expected,
    const TString& substring = {})
{
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, expected, result->ErrorMessage);
    if (!substring.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(result->ErrorMessage, substring);
    }
}

NKikimrPQ::TPQTabletConfig MakeConfigWithDlq(
    const TString& consumerName,
    const TString& dlq,
    bool enabled = true,
    NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy policy = NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE)
{
    NKikimrPQ::TPQTabletConfig config;
    auto* consumer = config.AddConsumers();
    consumer->SetName(consumerName);
    consumer->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer->SetDeadLetterPolicyEnabled(enabled);
    consumer->SetDeadLetterPolicy(policy);
    consumer->SetDeadLetterQueue(dlq);
    return config;
}

} // namespace

Y_UNIT_TEST_SUITE(TCheckDlqTopicsHelpers) {

    Y_UNIT_TEST(CollectSkipsDisabledSqsEmptyAndStreaming) {
        NKikimrPQ::TPQTabletConfig config;

        {
            auto* c = config.AddConsumers();
            c->SetName("streaming");
            c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
            c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
            c->SetDeadLetterPolicyEnabled(true);
            c->SetDeadLetterQueue("dlq-streaming");
        }
        {
            auto* c = config.AddConsumers();
            c->SetName("disabled");
            c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
            c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
            c->SetDeadLetterPolicyEnabled(false);
            c->SetDeadLetterQueue("dlq-disabled");
        }
        {
            auto* c = config.AddConsumers();
            c->SetName("sqs");
            c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
            c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
            c->SetDeadLetterPolicyEnabled(true);
            c->SetDeadLetterQueue("sqs://account/queue");
        }
        {
            auto* c = config.AddConsumers();
            c->SetName("empty");
            c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
            c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
            c->SetDeadLetterPolicyEnabled(true);
            c->SetDeadLetterQueue("");
        }
        {
            auto* c = config.AddConsumers();
            c->SetName("delete");
            c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
            c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
            c->SetDeadLetterPolicyEnabled(true);
            c->SetDeadLetterQueue("dlq-delete");
        }

        UNIT_ASSERT(CollectDlqTopicPaths(config, "/Root").empty());
    }

    Y_UNIT_TEST(CollectNormalizesRelativeAndAbsolutePaths) {
        auto config = MakeConfigWithDlq("c1", "dlq");
        auto* c2 = config.AddConsumers();
        c2->SetName("c2");
        c2->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        c2->SetDeadLetterPolicyEnabled(true);
        c2->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        c2->SetDeadLetterQueue("/Root/dlq");

        const auto paths = CollectDlqTopicPaths(config, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1u);
        UNIT_ASSERT(paths.contains("/Root/dlq"));
    }

    Y_UNIT_TEST(CollectNewPathsIgnoresUnchangedDlq) {
        const auto oldConfig = MakeConfigWithDlq("c1", "dlq1");
        auto newConfig = MakeConfigWithDlq("c1", "dlq1");
        auto* c2 = newConfig.AddConsumers();
        c2->SetName("c2");
        c2->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        c2->SetDeadLetterPolicyEnabled(true);
        c2->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        c2->SetDeadLetterQueue("dlq2");

        const auto paths = CollectNewDlqTopicPaths(newConfig, oldConfig, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1u);
        UNIT_ASSERT(paths.contains("/Root/dlq2"));
    }

    Y_UNIT_TEST(CollectNewPathsOnEnableExistingQueue) {
        const auto oldConfig = MakeConfigWithDlq("c1", "dlq", false);
        const auto newConfig = MakeConfigWithDlq("c1", "dlq", true);

        const auto paths = CollectNewDlqTopicPaths(newConfig, oldConfig, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1u);
        UNIT_ASSERT(paths.contains("/Root/dlq"));
    }

    Y_UNIT_TEST(FactoryReturnsNullWhenNoNewDlq) {
        const NActors::TActorId parent;
        const NKikimrPQ::TPQTabletConfig empty;
        const auto config = MakeConfigWithDlq("c1", "dlq1");

        UNIT_ASSERT(!CreateCheckDlqTopicsActorIfNeeded(parent, "/Root", empty, empty, {}));
        UNIT_ASSERT(!CreateCheckDlqTopicsActorIfNeeded(parent, "/Root", config, config, {}));

        NActors::IActor* actor = CreateCheckDlqTopicsActorIfNeeded(parent, "/Root", config, empty, {});
        UNIT_ASSERT(actor);
        delete actor;
    }

}

Y_UNIT_TEST_SUITE(TCheckDlqTopicsActor) {

    Y_UNIT_TEST(EmptyConfigSucceedsWithoutActor) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);

        AssertStatus(CheckDlq(setup->GetRuntime(), {}), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ExistingTopicSucceedsWithoutToken) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/topic1")),
            Ydb::StatusIds::SUCCESS
        );
    }

    Y_UNIT_TEST(MissingTopicIsSchemeError) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/missing")),
            Ydb::StatusIds::SCHEME_ERROR,
            "does not exist"
        );
    }

    Y_UNIT_TEST(TableIsBadRequest) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/table1")),
            Ydb::StatusIds::BAD_REQUEST,
            "must be a topic"
        );
    }

    Y_UNIT_TEST(CdcStreamIsBadRequest) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/table1/feed")),
            Ydb::StatusIds::BAD_REQUEST,
            "CDC stream cannot be used as a dead letter queue"
        );
    }

    Y_UNIT_TEST(CdcStreamImplIsBadRequest) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/table1/feed/streamImpl")),
            Ydb::StatusIds::BAD_REQUEST,
            "CDC stream cannot be used as a dead letter queue"
        );
    }

    Y_UNIT_TEST(SucceedsWithOnlyAlterSchema) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        const TString userSid = "user1@builtin";
        NACLib::TDiffACL acl;
        acl.SetInterruptInheritance(true);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::AlterSchema, userSid);
        ModifyTopicAcl(*setup, "topic1", acl);

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/topic1"), MakeUserToken(userSid)),
            Ydb::StatusIds::SUCCESS
        );
    }

    Y_UNIT_TEST(SucceedsWithOnlyUpdateRow) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        const TString userSid = "user1@builtin";
        NACLib::TDiffACL acl;
        acl.SetInterruptInheritance(true);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::UpdateRow, userSid);
        ModifyTopicAcl(*setup, "topic1", acl);

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/topic1"), MakeUserToken(userSid)),
            Ydb::StatusIds::SUCCESS
        );
    }

    Y_UNIT_TEST(FailsWithoutAccess) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        const TString userSid = "user1@builtin";
        NACLib::TDiffACL acl;
        acl.SetInterruptInheritance(true);
        ModifyTopicAcl(*setup, "topic1", acl);

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/topic1"), MakeUserToken(userSid)),
            Ydb::StatusIds::UNAUTHORIZED,
            "AlterSchema or UpdateRow"
        );
    }

    Y_UNIT_TEST(FailsWithOnlySelectRow) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        const TString userSid = "user1@builtin";
        NACLib::TDiffACL acl;
        acl.SetInterruptInheritance(true);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::SelectRow, userSid);
        ModifyTopicAcl(*setup, "topic1", acl);

        AssertStatus(
            CheckDlq(setup->GetRuntime(), MakeConfigWithDlq("c1", "/Root/topic1"), MakeUserToken(userSid)),
            Ydb::StatusIds::UNAUTHORIZED,
            "AlterSchema or UpdateRow"
        );
    }

    Y_UNIT_TEST(MixedPathsFailsOnFirstBadTarget) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableLogs(*setup);
        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        auto config = MakeConfigWithDlq("c1", "/Root/topic1");
        auto* c2 = config.AddConsumers();
        c2->SetName("c2");
        c2->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        c2->SetDeadLetterPolicyEnabled(true);
        c2->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        c2->SetDeadLetterQueue("/Root/table1");

        auto result = CheckDlq(setup->GetRuntime(), config);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::BAD_REQUEST, result->ErrorMessage);
        UNIT_ASSERT_STRING_CONTAINS(result->ErrorMessage, "must be a topic");
    }

}

} // namespace NKikimr::NPQ::NSchema
