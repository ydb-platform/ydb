#include "describer.h"

#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
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
        absl::flat_hash_set<TString> topics,
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
        absl::flat_hash_set<TString> topics,
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

    absl::flat_hash_map<TString, NDescriber::TTopicInfo> WaitResult(NActors::TTestActorRuntime& runtime) {
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

    void TestSleep(NActors::TTestActorRuntime& runtime, TDuration duration) {
        if (runtime.IsRealThreads()) {
            Sleep(duration);
        } else {
            runtime.SimulateSleep(duration);
        }
    }

    void WaitDatabaseRunning(NActors::TTestActorRuntime& runtime, const TString& path) {
        using namespace NKikimr::NConsole;

        Ydb::Cms::GetDatabaseStatusResult status;
        const TActorId edgeActor = runtime.AllocateEdgeActor();
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (TInstant::Now() < deadline) {
            auto request = std::make_unique<TEvConsole::TEvGetTenantStatusRequest>();
            request->Record.MutableRequest()->set_path(path);
            runtime.SendToPipe(MakeConsoleID(), edgeActor, request.release(), 0, GetPipeConfigWithRetries());

            auto response = runtime.GrabEdgeEvent<TEvConsole::TEvGetTenantStatusResponse>(edgeActor, TDuration::Seconds(5));
            if (response) {
                response->Get()->Record.GetResponse().operation().result().UnpackTo(&status);
                if (status.state() == Ydb::Cms::GetDatabaseStatusResult::RUNNING) {
                    return;
                }
            }
            TestSleep(runtime, TDuration::MilliSeconds(100));
        }
        UNIT_FAIL(TStringBuilder() << "Database " << path << " is not RUNNING, last status:\n" << status.DebugString());
    }

    void PrepareNodeChannelProfilesForPool(NActors::TTestActorRuntime& runtime, ui32 nodeIdx, const TString& poolKind) {
        auto& appData = runtime.GetAppData(nodeIdx);
        UNIT_ASSERT(appData.ChannelProfiles);
        // Nodes share ChannelProfiles by default; clone so tenant schemeshard binds to this DB's pool.
        auto profiles = MakeIntrusive<TChannelProfiles>();
        profiles->Profiles = appData.ChannelProfiles->Profiles;
        for (auto& profile : profiles->Profiles) {
            for (auto& channel : profile.Channels) {
                channel.PoolKind = poolKind;
            }
        }
        appData.ChannelProfiles = profiles;
    }

    void CreateDedicatedDatabase(::NPersQueue::TTestServer& server, const TString& path, ui32 dynamicNodeIdx) {
        auto& runtime = *server.CleverServer->GetRuntime();

        using TEvCreateDatabaseRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
            Ydb::Cms::CreateDatabaseRequest,
            Ydb::Cms::CreateDatabaseResponse>;

        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(path);
        auto* storage = request.mutable_resources()->add_storage_units();
        // Pool kind must match AddStoragePoolType(...) for this tenant path.
        storage->set_unit_kind(path);
        storage->set_count(1);

        const auto response = NKikimr::NRpcService::DoLocalRpc<TEvCreateDatabaseRequest>(
            std::move(request), "", "", runtime.GetActorSystem(0), true).ExtractValueSync();
        UNIT_ASSERT_C(response.operation().ready(), response.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.ShortDebugString());

        PrepareNodeChannelProfilesForPool(runtime, dynamicNodeIdx, path);
        server.CleverServer->SetupDynamicLocalService(dynamicNodeIdx, path);
        WaitDatabaseRunning(runtime, path);
    }

    void SetPqChannelPoolKind(NActors::TTestActorRuntime& runtime, const TString& poolKind) {
        for (auto& profile : *runtime.GetAppData().PQConfig.MutableChannelProfiles()) {
            profile.SetPoolKind(poolKind);
        }
    }

    void ExecuteDDLInDatabase(const TString& endpoint, const TString& database, const TString& query) {
        TDriver driver(TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database)
            .SetAuthToken("root@builtin")
            .SetDiscoveryMode(EDiscoveryMode::Off));
        TQueryClient client(driver);
        auto session = client.GetSession().GetValueSync().GetSession();

        Cerr << "DDL [" << database << "]: " << query << Endl << Flush;
        auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        driver.Stop(true);
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
        UNIT_ASSERT_C(topicInfo.Names.IsValid(), topicInfo.Names.GetReason());
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Names.GetClientsideName(), "topic1");
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Names.GetPrimaryPath(), "/Root/topic1");
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
        UNIT_ASSERT_C(topicInfo.Names.IsValid(), topicInfo.Names.GetReason());
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Names.GetClientsideName(), "/Root/table1/feed");
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
        UNIT_ASSERT_VALUES_EQUAL(NDescriber::Convert(NDescriber::EStatus::BAD_REQUEST), Ydb::StatusIds::BAD_REQUEST);
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
        UNIT_ASSERT(NDescriber::Description(path, NDescriber::EStatus::BAD_REQUEST).Contains("Invalid topic name"));
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

    // -------------------------------------------------------------------------
    // FederationRoot / federation-style paths
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(TopicWithFederationRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        const TString federationRoot = "/Root/Federation";
        const TString account = "account";
        const TString shortTopicName = account + "/topic1";
        const TString fullTopicPath = federationRoot + "/" + shortTopicName;

        ExecuteDDL(*setup, "CREATE TOPIC `Federation/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(federationRoot);

        StartDescribe(runtime, {shortTopicName});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains(shortTopicName));
        auto& topicInfo = topics[shortTopicName];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, fullTopicPath);
    }

    Y_UNIT_TEST(TopicWithFederationRootAbsoluteDatabasePrefixedPath) {
        // originalPath includes DatabasePath (/Root/...). Federation retry must strip
        // that prefix, not append FederationRoot + /Root/account/...
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        const TString federationRoot = "/Root/Federation";
        const TString absoluteTopicName = "/Root/account/topic1";
        const TString fullTopicPath = federationRoot + "/account/topic1";

        ExecuteDDL(*setup, "CREATE TOPIC `Federation/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(federationRoot);

        StartDescribe(runtime, {absoluteTopicName});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains(absoluteTopicName));
        auto& topicInfo = topics[absoluteTopicName];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, fullTopicPath);
    }

    Y_UNIT_TEST(TopicWithFederationRootLeadingSlash) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        const TString federationRoot = "/Root/Federation";
        const TString shortTopicName = "/account/topic";
        const TString fullTopicPath = federationRoot + "/account/topic";

        ExecuteDDL(*setup, "CREATE TOPIC `Federation/account/topic`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(federationRoot);

        StartDescribe(runtime, {shortTopicName});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains(shortTopicName));
        auto& topicInfo = topics[shortTopicName];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, fullTopicPath);
    }

    Y_UNIT_TEST(TopicWithFederationRootNotFound) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/Federation");

        StartDescribe(runtime, {"account/topic_not_exists"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("account/topic_not_exists"));
        auto& topicInfo = topics["account/topic_not_exists"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(TopicWithFederationRootIgnoredForFirstClassCitizen) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        const TString federationRoot = "/Root/Federation";
        const TString shortTopicName = "account/topic1";

        ExecuteDDL(*setup, "CREATE TOPIC `Federation/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(federationRoot);

        StartDescribe(runtime, {shortTopicName});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains(shortTopicName));
        auto& topicInfo = topics[shortTopicName];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(TopicWithFederationRootMultipleAccounts) {
        auto settings = NKikimr::NPersQueueTests::PQSettings(0, 1);
        settings.SetNodeCount(1);
        settings.SetDynamicNodeCount(2);
        settings.AddStoragePoolType("/Root/account1");
        settings.AddStoragePoolType("/Root/account2");
        // Create topics with FICC=true; Federation describe path requires FICC=false later.
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        settings.PQConfig.SetRoot("/Root");
        settings.PQConfig.SetDatabase("/Root");
        settings.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root");

        ::NPersQueue::TTestServer server(settings, false);
        server.StartServer(false);
        server.AnnoyingClient->GrantConnect("root@builtin");
        server.EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        const ui32 firstDynamicNode = server.CleverServer->StaticNodes();
        CreateDedicatedDatabase(server, "/Root/account1", firstDynamicNode);
        CreateDedicatedDatabase(server, "/Root/account2", firstDynamicNode + 1);

        auto& runtime = *server.GetRuntime();
        // CREATE TOPIC embeds PQ channel PoolKind from AppData; match each tenant's storage pool.
        SetPqChannelPoolKind(runtime, "/Root/account1");
        ExecuteDDLInDatabase(server.Endpoint, "/Root/account1", "CREATE TOPIC topic");
        SetPqChannelPoolKind(runtime, "/Root/account2");
        ExecuteDDLInDatabase(server.Endpoint, "/Root/account2", "CREATE TOPIC topic");

        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

        // account1/topic → Path=/Root/account1/topic, NavigateDatabase=/Root/account1.
        StartDescribe(runtime, {"account1/topic", "account2/topic"}, {}, "/Root");
        auto topics = WaitResult(runtime);

        UNIT_ASSERT_VALUES_EQUAL(topics.size(), 2);

        UNIT_ASSERT(topics.contains("account1/topic"));
        UNIT_ASSERT_VALUES_EQUAL(topics["account1/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["account1/topic"].RealPath, "/Root/account1/topic");

        UNIT_ASSERT(topics.contains("account2/topic"));
        UNIT_ASSERT_VALUES_EQUAL(topics["account2/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["account2/topic"].RealPath, "/Root/account2/topic");
    }

    Y_UNIT_TEST(TopicWithFederationRootSingleComponentPath) {
        // Federation retry needs account/topic shape; a single component must not
        // be rewritten under FederationRoot.
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC `Federation/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/Federation");

        StartDescribe(runtime, {"topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["topic1"].Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(CDCWithFederationRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE `Federation/account/table1` (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE `Federation/account/table1` ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/Federation");

        StartDescribe(runtime, {"account/table1/feed"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("account/table1/feed"));
        auto& topicInfo = topics["account/table1/feed"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStream, true);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStreamName, "feed");
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/Federation/account/table1/feed/streamImpl");
    }

    Y_UNIT_TEST(CDCWithEmptyDatabase) {
        // Same empty-Database contract as fetch/API callers (TFetchRequestTests.CDC).
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/table1/feed"}, {}, /*databasePath=*/"");
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1/feed"));
        auto& topicInfo = topics["/Root/table1/feed"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/table1/feed/streamImpl");
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStream, true);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStreamName, "feed");
    }

    Y_UNIT_TEST(TopicWithEmptyDatabaseAbsolutePath) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/topic1"}, {}, /*databasePath=*/"");
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/topic1"].RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(NotTopicWithDescribeAccess) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, "user1@staff");
        setup->GetServer().AnnoyingClient->ModifyACL("/Root", "table1", acl.SerializeAsString());

        auto& runtime = setup->GetRuntime();
        auto settings = WithToken(
            MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{}),
            NACLib::EAccessRights::SelectRow
        );
        StartDescribe(runtime, {"/Root/table1"}, settings);
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1"));
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1"].Status, NDescriber::EStatus::NOT_TOPIC);
    }

    Y_UNIT_TEST(MultipleCDC) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed1 WITH (FORMAT = 'JSON', MODE = 'UPDATES')");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed2 WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        StartDescribe(runtime, {"/Root/table1/feed1", "/Root/table1/feed2"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT_VALUES_EQUAL(topics.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed1"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed1"].CdcStream, true);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed1"].RealPath, "/Root/table1/feed1/streamImpl");
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed2"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed2"].CdcStream, true);
        UNIT_ASSERT_VALUES_EQUAL(topics["/Root/table1/feed2"].RealPath, "/Root/table1/feed2/streamImpl");
    }

    // -------------------------------------------------------------------------
    // Legacy rt3 / short names via nameresolver
    // -------------------------------------------------------------------------

    Y_UNIT_TEST(LegacyRt3TopicWithFederationRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        const TString federationRoot = "/Root/Federation";
        ExecuteDDL(*setup, "CREATE TOPIC `Federation/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(federationRoot);

        StartDescribe(runtime, {"rt3.dc1--account--topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("rt3.dc1--account--topic1"));
        auto& topicInfo = topics["rt3.dc1--account--topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, federationRoot + "/account/topic1");
    }

    Y_UNIT_TEST(LegacyShortTopicWithFederationRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        const TString federationRoot = "/Root/Federation";
        ExecuteDDL(*setup, "CREATE TOPIC `Federation/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(federationRoot);

        StartDescribe(runtime, {"account--topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("account--topic1"));
        auto& topicInfo = topics["account--topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, federationRoot + "/account/topic1");
    }

    Y_UNIT_TEST(LegacyRt3CdcWithFederationRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        ExecuteDDL(*setup, "CREATE TABLE `Federation/account/table1` (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE `Federation/account/table1` ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/Federation");

        // Nested path in legacy form: @ → /
        StartDescribe(runtime, {"rt3.dc1--account@table1--feed"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("rt3.dc1--account@table1--feed"));
        auto& topicInfo = topics["rt3.dc1--account@table1--feed"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStream, true);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.CdcStreamName, "feed");
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/Federation/account/table1/feed/streamImpl");
    }

    Y_UNIT_TEST(LegacyNameBadRequest) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        EnableDescriberLogs(*setup);

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/Federation");

        StartDescribe(runtime, {"rt3.bad"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("rt3.bad"));
        UNIT_ASSERT_VALUES_EQUAL(topics["rt3.bad"].Status, NDescriber::EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(TopicInDatabaseWithSlashInName) {
        // Database path has an extra path component ("my/db"), so topic paths look like
        // /Root/my/db/... while the tenant boundary is /Root/my/db.
        const TString dbPath = "/Root/my/db";

        auto settings = NKikimr::NPersQueueTests::PQSettings(0, 1);
        settings.SetNodeCount(1);
        settings.SetDynamicNodeCount(1);
        settings.AddStoragePoolType(dbPath);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        settings.PQConfig.SetRoot("/Root");
        settings.PQConfig.SetDatabase("/Root");

        ::NPersQueue::TTestServer server(settings, false);
        server.StartServer(false);
        server.AnnoyingClient->GrantConnect("root@builtin");
        server.EnableLogs(
            {NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER},
            NActors::NLog::PRI_DEBUG
        );

        const ui32 firstDynamicNode = server.CleverServer->StaticNodes();
        CreateDedicatedDatabase(server, dbPath, firstDynamicNode);

        auto& runtime = *server.GetRuntime();
        SetPqChannelPoolKind(runtime, dbPath);
        ExecuteDDLInDatabase(server.Endpoint, dbPath, "CREATE TOPIC `my-topic`");
        ExecuteDDLInDatabase(server.Endpoint, dbPath, "CREATE TOPIC `my-dir/my-topic`");

        // Relative topic name + matching tenant database.
        {
            StartDescribe(runtime, {"my-topic"}, {}, dbPath);
            auto topics = WaitResult(runtime);
            UNIT_ASSERT(topics.contains("my-topic"));
            UNIT_ASSERT_VALUES_EQUAL(topics["my-topic"].Status, NDescriber::EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(topics["my-topic"].RealPath, "/Root/my/db/my-topic");
        }

        // Absolute topic path + matching tenant database.
        {
            const TString topic = "/Root/my/db/my-topic";
            StartDescribe(runtime, {topic}, {}, dbPath);
            auto topics = WaitResult(runtime);
            UNIT_ASSERT(topics.contains(topic));
            UNIT_ASSERT_VALUES_EQUAL(topics[topic].Status, NDescriber::EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(topics[topic].RealPath, "/Root/my/db/my-topic");
        }

        // Absolute topic path under /Root database (navigate from domain root).
        // SchemeCache with DatabaseName=/Root does not resolve into the dedicated
        // tenant schemeshard → NOT_FOUND (path exists only under /Root/my/db).
        {
            const TString topic = "/Root/my/db/my-topic";
            StartDescribe(runtime, {topic}, {}, "/Root");
            auto topics = WaitResult(runtime);
            UNIT_ASSERT(topics.contains(topic));
            UNIT_ASSERT_VALUES_EQUAL(topics[topic].Status, NDescriber::EStatus::NOT_FOUND);
        }

        // Topic in a subdirectory; DatabaseName points at that subdirectory
        // (not the tenant root /Root/my/db). SchemeCache still resolves the path
        // under the owning schemeshard → SUCCESS.
        {
            StartDescribe(runtime, {"my-topic"}, {}, "/Root/my/db/my-dir");
            auto topics = WaitResult(runtime);
            UNIT_ASSERT(topics.contains("my-topic"));
            UNIT_ASSERT_VALUES_EQUAL(topics["my-topic"].Status, NDescriber::EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(topics["my-topic"].RealPath, "/Root/my/db/my-dir/my-topic");
        }
    }

}

}
