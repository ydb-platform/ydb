#include <ydb/core/discovery/discovery.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

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

void ValidateDiscovererResponse(TTestActorRuntime& runtime, TActorId edge) {
    auto discoveryDataEvent = runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge)->Release();
    UNIT_ASSERT(discoveryDataEvent);

    auto discoveryData = UnpackDiscoveryData(discoveryDataEvent->CachedMessage);
    auto discoverySslData = UnpackDiscoveryData(discoveryDataEvent->CachedMessageSsl);

    UNIT_ASSERT_EQUAL(discoveryData.endpoints_size(), 1);
    UNIT_ASSERT_EQUAL(discoverySslData.endpoints_size(), 0);
}

namespace {

TBridgeInfo::TPtr MakeBridgeInfo(NKikimrBridge::TClusterState::EPileState secondaryState) {
    auto info = std::make_shared<TBridgeInfo>();
    info->Piles.resize(2);
    for (ui32 i = 0; i != 2; ++i) {
        auto& pile = info->Piles[i];
        pile.BridgePileId = TBridgePileId::FromPileIndex(i);
        pile.Name = i ? "secondary" : "primary";
        pile.State = i ? secondaryState : NKikimrBridge::TClusterState::SYNCHRONIZED;
        pile.IsPrimary = i == 0;
    }
    info->PrimaryPile = info->SelfNodePile = &info->Piles[0];
    return info;
}

TMap<TActorId, TEvStateStorage::TBoardInfoEntry> MakeBoardEntries() {
    TMap<TActorId, TEvStateStorage::TBoardInfoEntry> entries;
    for (bool ssl : {false, true}) {
        for (const TString pile : {"primary", "secondary"}) {
            NKikimrStateStorage::TEndpointBoardEntry entry;
            entry.SetAddress(pile);
            entry.SetPort(ssl ? 2136 : 2135);
            entry.SetSsl(ssl);
            entry.SetBridgePileName(pile);
            UNIT_ASSERT(entry.SerializeToString(&entries[TActorId(ssl + 1, pile)].Payload));
        }
    }
    return entries;
}

void CheckEndpoints(const TString& serialized, bool secondaryVisible, bool ssl) {
    const auto result = UnpackDiscoveryData(serialized);
    TSet<TString> addresses;
    for (const auto& endpoint : result.endpoints()) {
        UNIT_ASSERT_VALUES_EQUAL(endpoint.ssl(), ssl);
        addresses.insert(endpoint.address());
    }
    UNIT_ASSERT_VALUES_EQUAL(result.endpoints_size(), secondaryVisible ? 2 : 1);
    UNIT_ASSERT(addresses.contains("primary"));
    UNIT_ASSERT_VALUES_EQUAL(addresses.contains("secondary"), secondaryVisible);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(Discovery) {
    Y_UNIT_TEST(EndpointsFollowPileServingState) {
        using TState = NKikimrBridge::TClusterState;
        const auto nodeInfo = MakeHolder<TEvInterconnect::TEvNodeInfo>(1);
        for (auto state : {TState::SYNCHRONIZED, TState::HOT_STANDBY, TState::DISCONNECTED, TState::INACTIVE,
                TState::NOT_SYNCHRONIZED_1, TState::NOT_SYNCHRONIZED_2, TState::SUSPENDED}) {
            const bool secondaryVisible = state == TState::SYNCHRONIZED || state == TState::NOT_SYNCHRONIZED_2
                || state == TState::SUSPENDED;
            TCachedMessageData cache(MakeBoardEntries(), nodeInfo, MakeBridgeInfo(state));
            CheckEndpoints(cache.CachedMessage, secondaryVisible, false);
            CheckEndpoints(cache.CachedMessageSsl, secondaryVisible, true);
            if (state == TState::HOT_STANDBY) {
                const auto result = UnpackDiscoveryData(cache.CachedMessage);
                UNIT_ASSERT_VALUES_EQUAL(result.pile_states_size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(result.pile_states(1).pile_name(), "secondary");
                UNIT_ASSERT_VALUES_EQUAL(result.pile_states(1).state(), Ydb::Bridge::PileState::HOT_STANDBY);
            }
        }
        TCachedMessageData nonBridged(MakeBoardEntries(), nodeInfo, nullptr);
        CheckEndpoints(nonBridged.CachedMessage, true, false);
        CheckEndpoints(nonBridged.CachedMessageSsl, true, true);
    }

    Y_UNIT_TEST(PileStateChangesRefreshCachedEndpoints) {
        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.GetAppData(0).FeatureFlags.SetEnableSubscriptionsInDiscovery(true);
        const auto edge = runtime.AllocateEdgeActor();
        const auto cache = runtime.Register(CreateDiscoveryCache({}, edge));
        runtime.GrabEdgeEvent<TEvInterconnect::TEvGetNode>(edge);

        auto setState = [&](NKikimrBridge::TClusterState::EPileState state) {
            runtime.Send(cache, edge, new TEvNodeWardenStorageConfig(nullptr, true, MakeBridgeInfo(state)));
        };
        setState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        runtime.Send(cache, edge, new TEvInterconnect::TEvNodeInfo(runtime.GetNodeId(0)));
        auto board = MakeHolder<TEvStateStorage::TEvBoardInfo>(TEvStateStorage::TEvBoardInfo::EStatus::Ok,
            MakeEndpointsBoardPath("/Root"));
        board->InfoEntries = MakeBoardEntries();
        runtime.Send(cache, edge, board.Release());

        auto check = [&](bool secondaryVisible, bool serialized) {
            runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", serialized, edge, cache));
            const auto response = runtime.GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(edge);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, TEvStateStorage::TEvBoardInfo::EStatus::Ok);
            if (serialized) {
                CheckEndpoints(response->Get()->CachedMessage, secondaryVisible, false);
                CheckEndpoints(response->Get()->CachedMessageSsl, secondaryVisible, true);
            } else {
                const auto nodeInfo = MakeHolder<TEvInterconnect::TEvNodeInfo>(runtime.GetNodeId(0));
                TCachedMessageData data(response->Get()->InfoEntries, nodeInfo, response->Get()->BridgeInfo);
                CheckEndpoints(data.CachedMessage, secondaryVisible, false);
                CheckEndpoints(data.CachedMessageSsl, secondaryVisible, true);
            }
        };
        check(true, true);
        // No board update accompanies these transitions; both response paths must use the latest pile state.
        setState(NKikimrBridge::TClusterState::HOT_STANDBY);
        check(false, true);
        setState(NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        check(false, false);
        setState(NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        check(true, true);
    }

    Y_UNIT_TEST(DelayedNameserviceResponse) {
        TKikimrRunner kikimr(TKikimrSettings{});

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto edge = runtime.AllocateEdgeActor();
 
        auto cache = runtime.Register(CreateDiscoveryCache({}, edge));
        auto discoverer = runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", true, edge, cache));
        Y_UNUSED(discoverer);

        auto getNodeEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvGetNode>(edge)->Release();

        runtime.Send(GetNameserviceActorId(), edge, getNodeEvent.Release());

        auto nodeInfoEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeInfo>(edge)->Release();

        Sleep(TDuration::Seconds(1));

        runtime.Send(cache, edge, nodeInfoEvent.Release());

        ValidateDiscovererResponse(runtime, edge);
    }

    Y_UNIT_TEST(SecondDiscovererRequestBeforeNameserviceResponse) {
        TKikimrRunner kikimr(TKikimrSettings{});

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto edge = runtime.AllocateEdgeActor();

        auto discovererEdge1 = runtime.AllocateEdgeActor();
        auto discovererEdge2 = runtime.AllocateEdgeActor();

        auto cache = runtime.Register(CreateDiscoveryCache({}, edge));
        auto discoverer1 = runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", true, discovererEdge1, cache));
        Y_UNUSED(discoverer1);

        auto getNodeEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvGetNode>(edge)->Release();

        runtime.Send(GetNameserviceActorId(), edge, getNodeEvent.Release());

        auto nodeInfoEvent = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeInfo>(edge)->Release();

        Sleep(TDuration::Seconds(1));

        auto discoverer2 = runtime.Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", true, discovererEdge2, cache));
        Y_UNUSED(discoverer2);

        Sleep(TDuration::Seconds(1));

        runtime.Send(cache, edge, nodeInfoEvent.Release());

        ValidateDiscovererResponse(runtime, discovererEdge1);
        ValidateDiscovererResponse(runtime, discovererEdge2);
    }
}

}
