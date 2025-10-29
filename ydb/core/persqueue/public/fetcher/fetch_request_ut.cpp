#include "fetch_request_actor.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NPQ {
using namespace ::NPersQueue;
//using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(TFetchRequestTests) {
    void StartSchemeCache(TTestActorRuntime& runtime) {
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            auto* appData = &runtime.GetAppData(nodeIndex);

            auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, new ::NMonitoring::TDynamicCounters());

            IActor* schemeCache = CreateSchemeBoardSchemeCache(cacheConfig.Get());
            TActorId schemeCacheId = runtime.Register(schemeCache, nodeIndex);
            runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId, nodeIndex);
        }
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



    Y_UNIT_TEST(HappyWay) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);
        setup->Write("/Root/topic1", "Data 1-1", 1);
        setup->Write("/Root/topic1", "Data 1-2", 1);
        setup->Write("/Root/topic1", "Data 1-3", 1);

        setup->CreateTopic("topic2", "dc1", totalPartitions);
        setup->Write("/Root/topic1", "Data 2-1", 3);
        setup->Write("/Root/topic1", "Data 2-2", 3);

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/topic1", 1, 1, 10000};
        TPartitionFetchRequest p2{"/Root/topic2", 3, 0, 10000};
        TPartitionFetchRequest pbad{"/Root/topic2", 2, 1, 10000};

        TFetchRequestSettings settings{{}, NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER, {p1, p2, pbad}, 1000, 1000};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        runtime.DispatchEvents();
        
        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);
        Cerr << "Got event: " << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 3);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic1");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 3);
        }

        {
            auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic2");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 3);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
        }

        {
            auto& result = ev->Response.GetPartResult(2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/topic2");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 2);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_ERROR_TOO_BIG_OFFSET));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
        }
    }

    Y_UNIT_TEST(CDC) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        ExecuteDDL(*setup, "CREATE TABLE table1 (id Uint64, PRIMARY KEY (id))");
        ExecuteDDL(*setup, "ALTER TABLE table1 ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");
        ExecuteDDL(*setup, "INSERT INTO table1 (id) VALUES (1)");


        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/table1/feed", 0, 0, 10000};

        TFetchRequestSettings settings{{}, NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER, {p1}, 1000, 1000};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        runtime.DispatchEvents();

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);
        Cerr << "Got event: " << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 1);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetTopic(), "/Root/table1/feed");
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 1);
        }
    }

    Y_UNIT_TEST(SmallBytesRead) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);
        setup->Write("/Root/topic1", TString(2_KB, 'a'), 0);

        TPartitionFetchRequest p1 {"/Root/topic1", 0, 0, 1_KB};
        TPartitionFetchRequest p2 {"/Root/topic1", 1, 0, 1_KB};

        TFetchRequestSettings settings{
            .Database = {},
            .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
            .Partitions = {p1, p2},
            .MaxWaitTimeMs = 1000,
            .TotalMaxBytes = 100
        };

        auto edgeId = runtime.AllocateEdgeActor();
        auto fetchActorId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchActorId);
        runtime.DispatchEvents();
        
        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        Cerr << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);

        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::OK));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetResult(0).GetUncompressedSize(), 2_KB);
        }

        {
            auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }
    }

    Y_UNIT_TEST(EmptyTopic) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        setup->CreateTopic("topic1", "dc1", 2);

        TPartitionFetchRequest p1 {"/Root/topic1", 0, 0, 1_KB};
        TPartitionFetchRequest p2 {"/Root/topic1", 1, 0, 1_KB};

        TFetchRequestSettings settings{
            .Database = {},
            .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
            .Partitions = {p1, p2},
            .MaxWaitTimeMs = 100,
            .TotalMaxBytes = 100
        };

        auto edgeId = runtime.AllocateEdgeActor();
        auto fetchActorId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchActorId);
        runtime.DispatchEvents();

        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        Cerr << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);

        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 2);

        {
            auto& result = ev->Response.GetPartResult(0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 0);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }

        {
            auto& result = ev->Response.GetPartResult(1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetPartition(), 1);
            UNIT_ASSERT_VALUES_EQUAL(::NPersQueue::NErrorCode::EErrorCode_Name(result.GetReadResult().GetErrorCode()),
                ::NPersQueue::NErrorCode::EErrorCode_Name(::NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE));
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().GetMaxOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetReadResult().ResultSize(), 0);
        }
    }

    Y_UNIT_TEST(BadTopicName) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_TRACE);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/topic1", 1, 1, 10000};
        TPartitionFetchRequest p2{"/Root/topic2", 3, 0, 10000};

        TFetchRequestSettings settings{{}, NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER, {p1, p2}, 1000, 1000};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        
        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SCHEME_ERROR, ev->Message);
    }

    Y_UNIT_TEST(CheckAccess) {
        auto setup = std::make_shared<TTopicSdkTestSetup>(TEST_CASE_NAME);
        auto& runtime = setup->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::PQ_FETCH_REQUEST, NActors::NLog::EPriority::PRI_DEBUG);
        StartSchemeCache(runtime);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"/Root/topic1", 1, 1, 10000};

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
            setup->GetServer().AnnoyingClient->ModifyACL("/Root", "topic1", acl.SerializeAsString());

            auto goodToken = MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{});
            TFetchRequestSettings settings{
                .Database = {},
                .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
                .Partitions = {p1},
                .MaxWaitTimeMs = 100,
                .TotalMaxBytes = 10000,
                .RuPerRequest = false,
                .UserToken = goodToken
            };

            auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
            runtime.EnableScheduleForActor(fetchId);

            auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
            UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);
        }
        
        {
            auto badToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
            TFetchRequestSettings settings{
                .Database = {},
                .Consumer = NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER,
                .Partitions = {p1},
                .MaxWaitTimeMs = 100,
                .TotalMaxBytes = 10000,
                .RuPerRequest = false,
                .UserToken = badToken
            };

            auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
            runtime.EnableScheduleForActor(fetchId);
            
            auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
            UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::UNAUTHORIZED, ev->Message);
        }
    }
};

}
