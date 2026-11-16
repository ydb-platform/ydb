#include "describer.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
using namespace NPersQueue;
//using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(TDescriberTests) {

    class TPathCheckingSchemeCache : public NActors::TActorBootstrapped<TPathCheckingSchemeCache> {
    public:
        TPathCheckingSchemeCache(TString database, TString path, bool retry)
            : Database(std::move(database))
            , Path(std::move(path))
            , Retry(retry)
        {}

        void Bootstrap() {
            Become(&TPathCheckingSchemeCache::StateWork);
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
            auto request = std::move(ev->Get()->Request);
            UNIT_ASSERT_VALUES_EQUAL(request->DatabaseName, Database);
            for (auto& entry : request->ResultSet) {
                UNIT_ASSERT_VALUES_EQUAL(CanonizePath(JoinPath(entry.Path)), Path);
                if (Retry && !entry.SyncVersion) {
                    entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown;
                } else {
                    entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                    entry.Kind = NSchemeCache::TSchemeCacheNavigate::EKind::KindTopic;
                    auto info = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>();
                    info->Description.SetBalancerTabletID(42);
                    entry.PQGroupInfo = info;
                }
            }
            Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
            }
        }

    private:
        TString Database;
        TString Path;
        bool Retry;
    };

    void CheckPath(const TString& database, const TString& name, const TString& expected,
                   bool enabled = true, bool optIn = true, bool firstClass = true, bool retry = false) {
        NActors::TTestBasicRuntime runtime(1, false);
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData().FeatureFlags.SetEnableRelativePaths(enabled);
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(firstClass);
        runtime.RegisterService(MakeSchemeCacheID(), runtime.Register(new TPathCheckingSchemeCache(
            enabled && optIn && firstClass ? CanonizePath(database) : database, expected, retry)));
        auto edge = runtime.AllocateEdgeActor();
        runtime.Register(NDescriber::CreateDescriberActor(edge, database, {name}, {
            .EnableRelativePaths = optIn,
        }));
        auto response = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.at(name).Status, NDescriber::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.at(name).RealPath, expected);
        UNIT_ASSERT_VALUES_EQUAL(response->UsedSyncVersion, retry);
    }

    Y_UNIT_TEST(FccPathsUseFlagAndAllowLegacyOptOut) {
        for (bool enabled : {false, true}) {
            for (bool optIn : {false, true}) {
                for (bool firstClass : {false, true}) {
                    CheckPath("/Root/Db", "Root/Db/topic",
                        enabled && optIn && firstClass ? "/Root/Db/Root/Db/topic" : "/Root/Db/topic",
                        enabled, optIn, firstClass);
                    CheckPath("/Root/Db", "/other/topic",
                        enabled && optIn && firstClass ? "/other/topic" : "/Root/Db/other/topic",
                        enabled, optIn, firstClass);
                }
            }
        }
    }

    Y_UNIT_TEST(FccResourcePathsUseDatabaseAndLeadingSlash) {
        for (const TString database : {"/Root/Db", "Root/Db", "/Root/team/Db", "/Root/Root/Db", "/Other/Db", ""}) {
            for (const TString name : {"topic", "Db/topic", "Root2/topic", "Root/Db/topic",
                    "Root/Root/Db/topic", "Root/other/topic", "Other/topic"}) {
                CheckPath(database, name, CanonizePath(database) + "/" + name);
            }
            for (const TString name : {"/Root/other/topic", "/Root2/topic", "/rt3.dc1--account--topic", "/Root/LbCommunal/account/topic"}) {
                CheckPath(database, name, name);
            }
            CheckPath(database, "", CanonizePath(database));
        }
    }

    Y_UNIT_TEST(ResolvedPathsSurviveSyncRetry) {
        CheckPath("/Root/Db", "Root/Db/topic", "/Root/Db/Root/Db/topic", true, true, true, true);
        CheckPath("/Root/Db", "/other/topic", "/other/topic", true, true, true, true);
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

    void CreateActor(NActors::TTestActorRuntime& runtime, std::unordered_set<TString>&& topics) {
        auto edgeId = runtime.AllocateEdgeActor();
        auto describerId = runtime.Register(NDescriber::CreateDescriberActor(edgeId, "/Root", std::move(topics)));
        runtime.EnableScheduleForActor(describerId);
        runtime.DispatchEvents();
    }

    std::unordered_map<TString, NDescriber::TTopicInfo> WaitResult(NActors::TTestActorRuntime& runtime) {
        auto ev = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>();
        return std::move(ev->Topics);
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
}

}
