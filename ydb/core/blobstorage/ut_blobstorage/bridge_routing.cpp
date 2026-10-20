#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    // holds events of the designated type sent to bridge proxies of one node, keeping the latest one of every proxy
    class TBridgeProxyEventHold {
        TEnvironmentSetup& Env;
        const ui32 NodeId;
        const ui32 EventType;
        bool Holding = false;
        std::map<TActorId, std::unique_ptr<IEventHandle>> Held;

    public:
        TBridgeProxyEventHold(TEnvironmentSetup& env, ui32 nodeId, ui32 eventType)
            : Env(env)
            , NodeId(nodeId)
            , EventType(eventType)
        {}

        void Start() {
            Holding = true;
        }

        size_t GetNumHeld() const {
            return Held.size();
        }

        bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (Holding && nodeId == NodeId && ev->GetTypeRewrite() == EventType) {
                if (IActor *actor = Env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                    Held[ev->GetRecipientRewrite()] = std::move(ev);
                    return false;
                }
            }
            return true;
        }

        void Release() {
            Holding = false;
            for (auto& [recipient, ev] : std::exchange(Held, {})) {
                Env.Runtime->Send(new IEventHandle(recipient, ev->Sender, ev->ReleaseBase().Release()), NodeId);
            }
        }
    };

    TEnvironmentSetup::TSettings MakeTwoPileSettings() {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    std::vector<ui32> GetPileGroups(TEnvironmentSetup& env, ui32 groupId) {
        const auto info = GetGroupInfo(env, groupId);
        std::vector<ui32> res;
        for (const auto& pile : info.GetBridgeGroupState().GetPile()) {
            res.push_back(pile.GetGroupId());
        }
        return res;
    }

    // read the blob from the physical group of the pile, so that the bridge proxy can't restore it
    NKikimrProto::EReplyStatus ReadFromPileGroup(TEnvironmentSetup& env, ui32 pileGroupId, ui32 nodeId,
            const TLogoBlobID& id, const TString& data) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge,
            new TEvNodeWardenQueryGroupInfo(pileGroupId)), nodeId);
        auto info = env.WaitForEdgeActorEvent<TEvNodeWardenGroupInfo>(edge, false);
        UNIT_ASSERT_C(info->Get()->Record.HasGroup(), info->Get()->Record.DebugString());
        const ui32 generation = info->Get()->Record.GetGroup().GetGroupGeneration();
        env.Runtime->WrapInActorContext(edge, [&] {
            auto *request = new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead);
            request->ForceGroupGeneration = generation;
            SendToBSProxy(edge, pileGroupId, request);
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        const auto& msg = *res->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(msg.Status, NKikimrProto::OK, msg.ToString());
        UNIT_ASSERT_VALUES_EQUAL(msg.ResponseSz, 1);
        if (msg.Responses[0].Status == NKikimrProto::OK) {
            UNIT_ASSERT_VALUES_EQUAL(msg.Responses[0].Buffer.ConvertToString(), data);
        }
        return msg.Responses[0].Status;
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeRouting) {

    // the bridge proxy which knows the group state made for newer cluster state than it knows must not skip the pile
    // that takes no part in data quorum by its cluster state, as the pile may have got synced already
    Y_UNIT_TEST(StaleClusterStateWaits) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        constexpr ui32 nodeId = 2; // node of the pile staying connected; its bridge proxy keeps old cluster state
        TBridgeProxyEventHold hold(env, nodeId, TEvNodeWardenStorageConfig::EventType);
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            return hold.Filter(node, ev);
        };

        TDataChecker checker(env);
        checker.Write(groupId, nodeId); // start the bridge proxy on the node

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, nodeId);

        hold.Start();
        RejoinPile(env, 1);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(hold.GetNumHeld()); // the proxy has not got the cluster state where the pile is synced

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(!res, "put completed with stale cluster state: " << res->Get()->ToString());

        hold.Release();
        res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());

        // the blob acknowledged after the pile got synced has to be in it
        const auto& [writtenGroupId, id, data] = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroupId, groupId);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[0], 1, id, data), NKikimrProto::OK);
        checker.CheckAll(nodeId);
    }

    // the bridge proxy which knows newer cluster state than the group state it has may find no pile to take the request;
    // it must wait for newer group state then
    Y_UNIT_TEST(NoEligiblePileWaits) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();

        constexpr ui32 nodeId = 2; // node of the pile staying connected; its bridge proxy keeps old group state
        TBridgeProxyEventHold hold(env, nodeId, TEvBlobStorage::TEvConfigureProxy::EventType);
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            return hold.Filter(node, ev);
        };

        TDataChecker checker(env);
        checker.Write(groupId, nodeId); // start the bridge proxy on the node

        // the proxy gets the group state where pile 1 is not synced
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, nodeId);

        hold.Start();
        RejoinPile(env, 1);

        // pile 1 becomes primary, pile 0 (the only synced one in the group state of the proxy) stops taking part in data
        // quorum, while its nodes stay connected
        auto clusterState = QueryStorageConfig(env).GetClusterState();
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPromotedPile);
        clusterState.SetPerPileState(0, NKikimrBridge::TClusterState::SUSPENDED);
        clusterState.SetGeneration(clusterState.GetGeneration() + 1);
        const auto switchResult = InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(clusterState);
        });
        UNIT_ASSERT_EQUAL_C(switchResult.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            switchResult.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(hold.GetNumHeld()); // the proxy has not got the group state where pile 1 is synced

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        checker.SendPut(groupId, edge);
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(!res, "put completed with stale group state: " << res->Get()->ToString());

        hold.Release();
        res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        checker.CheckAll(nodeId);
    }

}
