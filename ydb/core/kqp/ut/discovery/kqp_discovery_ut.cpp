#include <ydb/core/discovery/discovery.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NDiscovery {

auto UnpackDiscoveryData(const TString& data) {
    Ydb::Discovery::ListEndpointsResponse leResponse;
    Ydb::Discovery::ListEndpointsResult leResult;
    auto ok = leResponse.ParseFromString(data);
    UNIT_ASSERT(ok);
    ok = leResponse.operation().result().UnpackTo(&leResult);
    UNIT_ASSERT(ok);
    return leResult;
}

Y_UNIT_TEST_SUITE(Discovery) {
    Y_UNIT_TEST(DelayedNameserviceResponse) {
        TKikimrRunner kikimr(TKikimrSettings{});

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto edge = runtime.AllocateEdgeActor();
 
        auto cache = runtime.Register(CreateDiscoveryCache({}, edge));
        auto discoverer = runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", edge, cache));
        Y_UNUSED(discoverer);

        auto getNodeEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvGetNode>(edge)->Release();

        runtime.Send(GetNameserviceActorId(), edge, getNodeEvent.Release());

        auto nodeInfoEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeInfo>(edge)->Release();

        Sleep(TDuration::Seconds(5));

        runtime.Send(cache, edge, nodeInfoEvent.Release());

        auto discoveryDataEvent = runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge)->Release();
        UNIT_ASSERT(discoveryDataEvent);

        auto* ev = discoveryDataEvent.Release();
        auto discoveryData = UnpackDiscoveryData(ev->CachedMessageData->CachedMessage);
        auto discoverySslData = UnpackDiscoveryData(ev->CachedMessageData->CachedMessageSsl);

        UNIT_ASSERT_EQUAL(discoveryData.endpoints_size(), 1);
        UNIT_ASSERT_EQUAL(discoverySslData.endpoints_size(), 0);
    }
}

}
