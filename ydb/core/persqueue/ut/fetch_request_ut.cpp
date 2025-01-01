#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/fetch_request_actor.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/testlib/tenant_runtime.h>

namespace NKikimr::NPQ {
using namespace NPersQueue;
//using namespace NYdb::NTopic;
using namespace NYdb::NPersQueue::NTests;


Y_UNIT_TEST_SUITE(TFetchRequestTests) {
    void StartSchemeCache(TTestActorRuntime& runtime) {
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            auto* appData = &runtime.GetAppData(nodeIndex);

            auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, new NMonitoring::TDynamicCounters());

            IActor* schemeCache = CreateSchemeBoardSchemeCache(cacheConfig.Get());
            TActorId schemeCacheId = runtime.Register(schemeCache, nodeIndex);
            runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId, nodeIndex);
        }
    }

    Y_UNIT_TEST(HappyWay) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        setup->GetServer().EnableLogs(
                { NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::PQ_FETCH_REQUEST },
                NActors::NLog::PRI_DEBUG
        );
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);
        setup->CreateTopic("topic2", "dc1", totalPartitions);
        auto pqClient = setup->GetPersQueueClient();
        auto settings1 = TWriteSessionSettings().Path("topic1").MessageGroupId("src-id").PartitionGroupId(2);
        auto settings2 = TWriteSessionSettings().Path("topic2").MessageGroupId("src-id").PartitionGroupId(4);
        auto ws1 = pqClient.CreateSimpleBlockingWriteSession(settings1);
        auto ws2 = pqClient.CreateSimpleBlockingWriteSession(settings2);
        
        ws1->Write("Data 1-1");
        ws1->Write("Data 1-2");
        ws1->Write("Data 1-3");
        
        ws2->Write("Data 2-1");
        ws2->Write("Data 2-2");

        ws1->Close();
        ws2->Close();


        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"Root/PQ/rt3.dc1--topic1", 1, 1, 10000};
        TPartitionFetchRequest p2{"Root/PQ/rt3.dc1--topic2", 3, 0, 10000};
        TPartitionFetchRequest pbad{"Root/PQ/rt3.dc1--topic2", 2, 1, 10000};

        TFetchRequestSettings settings{{}, {p1, p2, pbad}, 10000, 10000, {}};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        runtime.DispatchEvents();
        
        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);
        Cerr << "Got event: " << ev->Response.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Response.PartResultSize(), 3);
        for (const auto& part : ev->Response.GetPartResult()) {
            if (part.GetTopic().Contains("topic1")) {
                UNIT_ASSERT(part.GetReadResult().GetErrorCode() == NPersQueue::NErrorCode::EErrorCode::OK);
                UNIT_ASSERT_VALUES_EQUAL(part.GetPartition(), 1);
                UNIT_ASSERT_VALUES_EQUAL(part.GetReadResult().GetResult(0).GetOffset(), 1);
            } else {
                UNIT_ASSERT(part.GetTopic().Contains("topic2"));
                if (part.GetPartition() == 2) {
                    UNIT_ASSERT(part.GetReadResult().GetErrorCode() != NPersQueue::NErrorCode::EErrorCode::OK);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(part.GetPartition(), 3);
                    UNIT_ASSERT_VALUES_EQUAL(part.GetReadResult().GetResult(0).GetOffset(), 0);
                }
            }
        }
    }

    Y_UNIT_TEST(BadTopicName) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_TRACE);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"Root/PQ/rt3.dc1--topic1", 1, 1, 10000};
        TPartitionFetchRequest p2{"Root/PQ/rt3.dc1--topic2", 3, 0, 10000};

        TFetchRequestSettings settings{{}, {p1, p2}, 10000, 10000, {}};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        
        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SCHEME_ERROR, ev->Message);
    }

    Y_UNIT_TEST(CheckAccess) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto& runtime = setup->GetRuntime();
        StartSchemeCache(runtime);

        ui32 totalPartitions = 5;
        setup->CreateTopic("topic1", "dc1", totalPartitions);
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
        setup->GetServer().AnnoyingClient->ModifyACL("/Root/PQ", "rt3.dc1--topic1", acl.SerializeAsString());

        auto edgeId = runtime.AllocateEdgeActor();
        TPartitionFetchRequest p1{"Root/PQ/rt3.dc1--topic1", 1, 1, 10000};
        auto goodToken = MakeIntrusiveConst<NACLib::TUserToken>("user1@staff", TVector<TString>{});
        auto badToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
        TFetchRequestSettings settings{{}, {p1}, 10000, 10000, {}, goodToken};
        auto fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        
        auto ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::SUCCESS, ev->Message);
        settings.User = badToken;

        fetchId = runtime.Register(CreatePQFetchRequestActor(settings, MakeSchemeCacheID(), edgeId));
        runtime.EnableScheduleForActor(fetchId);
        
        ev = runtime.GrabEdgeEvent<TEvPQ::TEvFetchResponse>();
        UNIT_ASSERT_C(ev->Status == Ydb::StatusIds::UNAUTHORIZED, ev->Message);

    }
};

}
