#include "describer.h"

#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
using namespace NPersQueue;
using namespace NYdb::NTopic::NTests;
using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(TDescriberTests) {

    void EnableDescriberLogs(TTopicSdkTestSetup& setup) {
        setup.GetServer().EnableLogs(
            {NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER},
            NActors::NLog::PRI_DEBUG
        );
    }

    void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
        TDriver driver(setup.MakeDriverConfig());
        TQueryClient client(driver);
        auto session = client.GetSession().GetValueSync().GetSession();

        Cerr << "DDL: " << query << Endl << Flush;
        auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        driver.Stop(true);
    }

    void ModifyTopicAcl(TTopicSdkTestSetup& setup, const TString& topicName, const NACLib::TDiffACL& acl) {
        setup.GetServer().AnnoyingClient->ModifyACL("/Root", topicName, acl.SerializeAsString());
    }

    struct TDescribeRun {
        TActorId EdgeId;
        TActorId DescriberId;
    };

    TDescribeRun RegisterDescribe(
        NActors::TTestActorRuntime& runtime,
        std::unordered_set<TString> topics,
        const NDescriber::TDescribeSettings& settings = {},
        const TString& databasePath = "/Root")
    {
        TDescribeRun run;
        run.EdgeId = runtime.AllocateEdgeActor();
        run.DescriberId = runtime.Register(NDescriber::CreateDescriberActor(
            run.EdgeId,
            databasePath,
            std::move(topics),
            settings
        ));
        runtime.EnableScheduleForActor(run.DescriberId);
        return run;
    }

    TDescribeRun StartDescribe(
        NActors::TTestActorRuntime& runtime,
        std::unordered_set<TString> topics,
        const NDescriber::TDescribeSettings& settings = {},
        const TString& databasePath = "/Root")
    {
        auto run = RegisterDescribe(runtime, std::move(topics), settings, databasePath);
        runtime.DispatchEvents();
        return run;
    }

    THolder<NDescriber::TEvDescribeTopicsResponse> WaitResponse(NActors::TTestActorRuntime& runtime) {
        return runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>();
    }

    std::unordered_map<TString, NDescriber::TTopicInfo> WaitResult(NActors::TTestActorRuntime& runtime) {
        auto ev = WaitResponse(runtime);
        return std::move(ev->Topics);
    }

    // After ModifyACL scheme-cache may still hold a stale ACL without SyncVersion.
    NDescriber::TDescribeSettings WithToken(
        TIntrusiveConstPtr<NACLib::TUserToken> token,
        NDescriber::TAccessRights accessRights)
    {
        return NDescriber::TDescribeSettings{
            .UserToken = std::move(token),
            .AccessRights = accessRights,
            .ForceSyncVersion = true,
        };
    }

    // -------------------------------------------------------------------------
    // Existing path / status tests
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(TopicExists) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        auto& topicInfo = topics["/Root/topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(TopicNotExists) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/topic_not_exists"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic_not_exists"));
        auto& topicInfo = topics["/Root/topic_not_exists"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(TopicNotTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/table1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1"));
        auto& topicInfo = topics["/Root/table1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_TOPIC);
    }

    Y_UNIT_TEST(CDC) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/table1/feed"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1/feed"));
        auto& topicInfo = topics["/Root/table1/feed"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/table1/feed/streamImpl");
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStream, true);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStreamName, "feed");
    }

    Y_UNIT_TEST(TopicWithoutDatabase) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("topic1"));
        auto& topicInfo = topics["topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(TopicNotCanonizedPath) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"Root/topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("Root/topic1"));
        auto& topicInfo = topics["Root/topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/topic1");
    }

    // -------------------------------------------------------------------------
    // P0 — authorization
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(AuthorizedSuccess) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
        ModifyTopicAcl(*setup, "topic1", acl);

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{}),
            NACLib::EAccessRights::SelectRow
        );
        StartDescribe(runtime, {"/Root/topic1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        auto& topicInfo = topics["/Root/topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT(topicInfo.Info);
        UNIT_ASSERT(topicInfo.Self);
        UNIT_ASSERT(topicInfo.SecurityObject);
    }

    Y_UNIT_TEST(UnauthorizedNoDescribe) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
            NACLib::EAccessRights::SelectRow
        );
        StartDescribe(runtime, {"/Root/topic1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST(UnauthorizedWithDescribe) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1@staff");
        ModifyTopicAcl(*setup, "topic1", acl);

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{}),
            NACLib::EAccessRights::SelectRow
        );
        StartDescribe(runtime, {"/Root/topic1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS);
    }

    Y_UNIT_TEST(AccessOrAllows) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1@staff");
        ModifyTopicAcl(*setup, "topic1", acl);

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{}),
            NDescriber::TAccessRights(NACLib::SelectRow, NACLib::DescribeSchema)
        );
        StartDescribe(runtime, {"/Root/topic1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
    }

    Y_UNIT_TEST(AccessOrDenied) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
            NDescriber::TAccessRights(NACLib::SelectRow, NACLib::UpdateRow)
        );
        StartDescribe(runtime, {"/Root/topic1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST(NotTopicWithoutDescribe) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
            NACLib::EAccessRights::SelectRow
        );
        StartDescribe(runtime, {"/Root/table1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1"].Status, NDescriber::EStatus::UNAUTHORIZED);
    }

    Y_UNIT_TEST(CustomAccessRightsAlterSchema) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::AlterSchema, "user1@staff");
        ModifyTopicAcl(*setup, "topic1", acl);

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{}),
            NACLib::EAccessRights::AlterSchema
        );
        StartDescribe(runtime, {"/Root/topic1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
    }

    // -------------------------------------------------------------------------
    // P1 — sync
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(ForceSyncVersion) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        NDescriber::TDescribeSettings settings{
            .ForceSyncVersion = true,
        };
        StartDescribe(runtime, {"/Root/topic1"}, settings);
        auto ev = WaitResponse(runtime);

        UNIT_ASSERT(ev->UsedSyncVersion);
        UNIT_ASSERT(ev->Topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(ev->Topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
    }

    // Scheme-cache error / incomplete-topic branches that need a controllable
    // NavigateKeySetResult live in describer_fake_scheme_cache_ut.cpp
    // (TTestBasicRuntime with UseRealThreads=false).

    // -------------------------------------------------------------------------
    // P1 — Convert / Description helpers
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(ConvertStatuses) {
        UNIT_ASSERT_VALUES_EQUAL(NDescriber::Convert(NDescriber::EStatus::SUCCESS), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(NDescriber::Convert(NDescriber::EStatus::NOT_FOUND), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(NDescriber::Convert(NDescriber::EStatus::NOT_TOPIC), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(NDescriber::Convert(NDescriber::EStatus::UNAUTHORIZED), Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(
            NDescriber::Convert(NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS),
            Ydb::StatusIds::UNAUTHORIZED
        );
        UNIT_ASSERT_VALUES_EQUAL(NDescriber::Convert(NDescriber::EStatus::UNKNOWN_ERROR), Ydb::StatusIds::INTERNAL_ERROR);
    }

    Y_UNIT_TEST(DescriptionMessages) {
        const TString path = "/Root/topic1";

        UNIT_ASSERT(NDescriber::Description(path, NDescriber::EStatus::SUCCESS).Contains("successfully described"));
        UNIT_ASSERT(NDescriber::Description(path, NDescriber::EStatus::NOT_FOUND).Contains("does not exist"));
        UNIT_ASSERT(NDescriber::Description(path, NDescriber::EStatus::UNAUTHORIZED).Contains("does not exist"));
        UNIT_ASSERT(NDescriber::Description(path, NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS)
            .Contains("do not have access permissions to"));
        UNIT_ASSERT(!NDescriber::Description(path, NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS)
            .Contains("does not exist"));
        UNIT_ASSERT(NDescriber::Description(path, NDescriber::EStatus::NOT_TOPIC).Contains("is not a topic"));
        UNIT_ASSERT(NDescriber::Description(path, NDescriber::EStatus::UNKNOWN_ERROR).Contains("Error describing"));
    }

    // -------------------------------------------------------------------------
    // P2 — batch / CDC edges / payload
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(MultipleTopicsMixed) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/topic1", "/Root/missing", "/Root/table1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT_VALUES_EQUAL(topics.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/missing"].Status, NDescriber::EStatus::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1"].Status, NDescriber::EStatus::NOT_TOPIC);
    }

    Y_UNIT_TEST(MultipleWithCDC) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");
        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/topic1", "/Root/table1/feed"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT_VALUES_EQUAL(topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].CdcStream, false);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed"].CdcStream, true);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed"].RealPath, "/Root/table1/feed/streamImpl");
    }

    Y_UNIT_TEST(CDCNotFound) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/table1/missing_feed"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1/missing_feed"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/missing_feed"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(CDCUnauthorized) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
            NACLib::EAccessRights::SelectRow
        );
        StartDescribe(runtime, {"/Root/table1/feed"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1/feed"));
        auto status = topics["/Root/table1/feed"].Status;
        UNIT_ASSERT_C(
            status == NDescriber::EStatus::UNAUTHORIZED ||
                status == NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS,
            static_cast<int>(status)
        );
    }

    Y_UNIT_TEST(EmptyTopics) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {});
        auto ev = WaitResponse(runtime);

        UNIT_ASSERT(ev->Topics.empty());
        UNIT_ASSERT(!ev->UsedSyncVersion);
    }

    Y_UNIT_TEST(SuccessPayload) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        auto& topicInfo = topics["/Root/topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/topic1");
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStream, false);
        UNIT_ASSERT(topicInfo.CdcStreamName.empty());
        UNIT_ASSERT(topicInfo.Info);
        UNIT_ASSERT(topicInfo.Self);
        UNIT_ASSERT(topicInfo.SecurityObject);
        UNIT_ASSERT(topicInfo.Info->Description.GetBalancerTabletID() != 0);
    }

    // -------------------------------------------------------------------------
    // P3 — path edge cases / poison
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(DirectoryIsNotTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root"].Status, NDescriber::EStatus::NOT_TOPIC);
    }

    Y_UNIT_TEST(DoubleSlashPath) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root//topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root//topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root//topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root//topic1"].RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(TrailingSlashPath) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/topic1/"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1/"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1/"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1/"].RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(PoisonBeforeResponse) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        auto run = RegisterDescribe(runtime, {"/Root/topic1"});
        runtime.Send(new IEventHandle(run.DescriberId, run.EdgeId, new TEvents::TEvPoison()));
        runtime.DispatchEvents();
        // Actor must handle poison without crashing the runtime.
    }

}

}
