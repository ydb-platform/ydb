#include "describer.h"

#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
using namespace NPersQueue;
//using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(TDescriberTests) {

    void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
        TDriver driver(setup.MakeDriverConfig());
        TQueryClient client(driver);
        auto session = client.GetSession().GetValueSync().GetSession();

        Cerr << "DDL: " << query << Endl << Flush;
        auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        driver.Stop(true);
    }

    void CreateActor(NActors::TTestActorRuntime& runtime, absl::flat_hash_set<TString>&& topics, const TString& databasePath = "/Root") {
        auto edgeId = runtime.AllocateEdgeActor();
        auto describerId = runtime.Register(NDescriber::CreateDescriberActor(edgeId, databasePath, std::move(topics)));
        runtime.EnableScheduleForActor(describerId);
        runtime.DispatchEvents();
    }

    absl::flat_hash_map<TString, NDescriber::TTopicInfo> WaitResult(NActors::TTestActorRuntime& runtime) {
        auto ev = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>();
        return std::move(ev->Topics);
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

    Y_UNIT_TEST(TopicExists) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {"/Root/topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic1"));
        auto& topicInfo = topics["/Root/topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(TopicNotExists) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {"/Root/topic_not_exists"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/topic_not_exists"));
        auto& topicInfo = topics["/Root/topic_not_exists"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(TopicNotTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {"/Root/table1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("/Root/table1"));
        auto& topicInfo = topics["/Root/table1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_TOPIC);
    }

    Y_UNIT_TEST(CDC) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {"/Root/table1/feed"});
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
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {"topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("topic1"));
        auto& topicInfo = topics["topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(TopicNotCanonizedPath) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        CreateActor(runtime, {"Root/topic1"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("Root/topic1"));
        auto& topicInfo = topics["Root/topic1"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, "/Root/topic1");
    }

    Y_UNIT_TEST(TopicWithLbUserDatabaseRoot) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        const TString dbRoot = "/Root/LbAccount";
        const TString account = "account";
        const TString shortTopicName = account + "/topic1";
        const TString fullTopicPath = dbRoot + "/" + shortTopicName;

        ExecuteDDL(*setup, "CREATE TOPIC `LbAccount/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(dbRoot);

        CreateActor(runtime, {shortTopicName});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains(shortTopicName));
        auto& topicInfo = topics[shortTopicName];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, fullTopicPath);
    }

    Y_UNIT_TEST(TopicWithLbUserDatabaseRootLeadingSlash) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        const TString dbRoot = "/Root/LbAccount";
        const TString shortTopicName = "/account/topic";
        const TString fullTopicPath = dbRoot + "/account/topic";

        ExecuteDDL(*setup, "CREATE TOPIC `LbAccount/account/topic`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(dbRoot);

        CreateActor(runtime, {shortTopicName});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains(shortTopicName));
        auto& topicInfo = topics[shortTopicName];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.RealPath, fullTopicPath);
    }

    Y_UNIT_TEST(TopicWithLbUserDatabaseRootNotFound) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/LbAccount");

        CreateActor(runtime, {"account/topic_not_exists"});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains("account/topic_not_exists"));
        auto& topicInfo = topics["account/topic_not_exists"];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(TopicWithLbUserDatabaseRootIgnoredForFirstClassCitizen) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_DESCRIBER },
                NActors::NLog::PRI_DEBUG
        );

        const TString dbRoot = "/Root/LbAccount";
        const TString shortTopicName = "account/topic1";

        ExecuteDDL(*setup, "CREATE TOPIC `LbAccount/account/topic1`");

        auto& runtime = setup->GetRuntime();
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
        runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(dbRoot);

        CreateActor(runtime, {shortTopicName});
        auto topics = WaitResult(runtime);

        UNIT_ASSERT(topics.contains(shortTopicName));
        auto& topicInfo = topics[shortTopicName];
        UNIT_ASSERT_VALUES_EQUAL(topicInfo.Status, NDescriber::EStatus::NOT_FOUND);
    }

    Y_UNIT_TEST(TopicWithLbUserDatabaseRootMultipleAccounts) {
        auto settings = NKikimr::NPersQueueTests::PQSettings(0, 1);
        settings.SetNodeCount(1);
        settings.SetDynamicNodeCount(2);
        settings.AddStoragePoolType("/Root/account1");
        settings.AddStoragePoolType("/Root/account2");
        // Create topics with FICC=true; LbRoot describe path requires FICC=false later.
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

        // Primary resolve under /Root misses into account tenants; LbRoot retries
        // with DatabaseName=/Root/account1 and /Root/account2.
        CreateActor(runtime, {"account1/topic", "account2/topic"}, "/Root");
        auto topics = WaitResult(runtime);

        UNIT_ASSERT_VALUES_EQUAL(topics.size(), 2);

        UNIT_ASSERT(topics.contains("account1/topic"));
        UNIT_ASSERT_VALUES_EQUAL(topics["account1/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["account1/topic"].RealPath, "/Root/account1/topic");

        UNIT_ASSERT(topics.contains("account2/topic"));
        UNIT_ASSERT_VALUES_EQUAL(topics["account2/topic"].Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(topics["account2/topic"].RealPath, "/Root/account2/topic");
    }
}

}
