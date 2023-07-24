#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/client/server/ic_nodes_cache_service.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/services/persqueue_v1/ut/test_utils.h>


namespace NKikimr::NPersQueueTests {

using namespace NIcNodeCache;

Y_UNIT_TEST_SUITE(TIcNodeCache) {
    Y_UNIT_TEST(GetNodesInfoTest) {
        NPersQueue::TTestServer server;
        auto* runtime = server.CleverServer->GetRuntime();
        runtime->GetAppData().FeatureFlags.SetEnableIcNodeCache(true);

        const auto edge = runtime->AllocateEdgeActor();
        runtime->RegisterService(CreateICNodesInfoCacheServiceId(), runtime->Register(
                CreateICNodesInfoCacheService(nullptr, TDuration::Seconds(1)))
        );
        auto makeRequest = [&]() {
            auto* request = new TEvICNodesInfoCache::TEvGetAllNodesInfoRequest();
            runtime->Send(CreateICNodesInfoCacheServiceId(), edge, request);
            auto ev = runtime->GrabEdgeEvent<TEvICNodesInfoCache::TEvGetAllNodesInfoResponse>();
            UNIT_ASSERT_VALUES_EQUAL(ev->Nodes->size(), 2);
        };
        makeRequest();
        for (auto i = 0u; i < 6; i++) {
            Sleep(TDuration::MilliSeconds(500));
            makeRequest();
        }
    }
};
} // NKikimr::NPersQueueTests