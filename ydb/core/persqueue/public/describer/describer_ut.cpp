#include "describer.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>


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

    void CreateActor(NActors::TTestActorRuntime& runtime, std::unordered_set<TString>&& topics) {
        auto edgeId = runtime.AllocateEdgeActor();
        auto describerId = runtime.Register(NDescriber::CreateDescriberActor(edgeId, "/Root", std::move(topics)));
        runtime.EnableScheduleForActor(describerId);
        runtime.DispatchEvents();
    }

    std::unordered_map<TString, NDescriber::TTopicInfo> WaitResult(NActors::TTestActorRuntime& runtime) {
        auto ev = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>();
        return ev->Topics;
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
    }
}

}
