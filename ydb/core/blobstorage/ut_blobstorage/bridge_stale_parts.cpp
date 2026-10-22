#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    // holds storage configs sent to BSC, keeping the latest one, so that BSC doesn't learn of cluster state changes (as
    // if it was restarting or slow) until they are released; parts of its groups in the piles that leave and get back
    // keep the SYNCED stage of their previous sync meanwhile
    class TBscConfigHold {
        TEnvironmentSetup& Env;
        bool Holding = false;
        std::unique_ptr<IEventHandle> Held;
        ui32 HeldNodeId = 0;

    public:
        explicit TBscConfigHold(TEnvironmentSetup& env)
            : Env(env)
        {}

        void Start() {
            Holding = true;
        }

        bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (Holding && ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                if (IActor *actor = Env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains("TBlobStorageController")) {
                    Held = std::move(ev);
                    HeldNodeId = nodeId;
                    return false;
                }
            }
            return true;
        }

        void Release() {
            Holding = false;
            UNIT_ASSERT(Held);
            const TActorId recipient = Held->GetRecipientRewrite();
            Env.Runtime->Send(new IEventHandle(recipient, Held->Sender, Held->ReleaseBase().Release()), HeldNodeId);
            Held.reset();
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

    // the part of the bridged group in the pile, as BSC knows it
    NKikimrBridge::TGroupState::TPile GetPart(TEnvironmentSetup& env, ui32 groupId, ui32 pileIndex) {
        const auto info = GetGroupInfo(env, groupId);
        const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(pileIndex));
        UNIT_ASSERT_C(pile, info.DebugString());
        return *pile;
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeStaleParts) {

    // the pile gets back while BSC doesn't know it has been out: the part of the pile is SYNCED by its previous sync, but
    // lacks the data written meanwhile; the bridge proxy must not take its reply as the answer
    Y_UNIT_TEST(StaleSyncedPartNotTrusted) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        constexpr ui32 nodeId = 2; // node of the pile staying connected
        TBscConfigHold bscHold(env);
        bool holdPrimaryReads = false;
        std::vector<std::unique_ptr<IEventHandle>> heldReads;
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (holdPrimaryReads && node == nodeId && ev->GetTypeRewrite() == TEvBlobStorage::TEvGet::EventType &&
                    ev->Recipient == MakeBlobStorageProxyID(pileGroups[0])) {
                heldReads.push_back(std::move(ev));
                return false;
            }
            return bscHold.Filter(node, ev);
        };

        TDataChecker checker(env);
        checker.Write(groupId, nodeId);

        bscHold.Start();
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId, nodeId); // reaches pile 0 only
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        env.Sim(TDuration::Seconds(10)); // let the node know the cluster state
        UNIT_ASSERT_EQUAL(GetPart(env, groupId, 1).GetStage(), NKikimrBridge::TGroupState::SYNCED); // not reset by BSC

        // the synchronized pile answers late
        holdPrimaryReads = true;
        const auto& written = checker.GetWritten().back();
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(written), groupId);
        const TLogoBlobID id = std::get<1>(written);
        const TString data = std::get<2>(written);
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Seconds(1));
        UNIT_ASSERT_C(!res, "read answered by the part of the pile that has been out: " << res->Get()->ToString());
        UNIT_ASSERT(!heldReads.empty());

        holdPrimaryReads = false;
        for (auto& ev : std::exchange(heldReads, {})) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        CheckGetResult(*res->Get(), data);

        // BSC learns of the cluster state, the pile gets synced
        bscHold.Release();
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        checker.CheckAll(nodeId);
        CheckPileData(env, checker, 1, 9);
    }

    // the group created while the pile is out gets the part in the pile which misses writes; the part has to be synced
    // when the pile gets back, rather than be SYNCED from the very beginning
    Y_UNIT_TEST(GroupCreatedWhilePileOut) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        CreatePoolAndGetGroup(env);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        std::optional<ui32> groupId;
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        for (const auto *group : GetPoolGroups(baseConfig, 2)) {
            if (group->GetIsProxyGroup()) {
                groupId = group->GetGroupId();
            }
        }
        UNIT_ASSERT(groupId);

        const auto part = GetPart(env, *groupId, 1);
        UNIT_ASSERT_UNEQUAL_C(part.GetStage(), NKikimrBridge::TGroupState::SYNCED, part.DebugString());
        const auto config = QueryStorageConfig(env);
        std::optional<ui64> becameUnsyncedGeneration;
        for (const auto& pss : config.GetClusterStateDetails().GetPileSyncState()) {
            if (TBridgePileId::FromProto(&pss, &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId) ==
                    TBridgePileId::FromPileIndex(1)) {
                becameUnsyncedGeneration = pss.GetBecameUnsyncedGeneration();
            }
        }
        UNIT_ASSERT(becameUnsyncedGeneration);
        UNIT_ASSERT_VALUES_EQUAL(part.GetBecameUnsyncedGeneration(), *becameUnsyncedGeneration);

        TDataChecker checker(env);
        checker.Write(*groupId); // reaches pile 0 only
        RejoinPile(env, 1);
        checker.Write(*groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 1, 9);
    }

    // assimilation and integrity checks take the answer of the synchronized pile only, so they go nowhere else: the part
    // of the pile being synced (shown here by the one not reset by BSC yet) may be degraded and fail them otherwise
    Y_UNIT_TEST(AssimilateAndCheckIntegritySkipUnsyncedPile) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        constexpr ui32 nodeId = 2; // node of the pile staying connected
        TBscConfigHold bscHold(env);
        ui32 numSentToPile1 = 0;
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if ((ev->GetTypeRewrite() == TEvBlobStorage::TEvAssimilate::EventType ||
                    ev->GetTypeRewrite() == TEvBlobStorage::TEvCheckIntegrity::EventType) &&
                    ev->Recipient == MakeBlobStorageProxyID(pileGroups[1])) {
                ++numSentToPile1;
            }
            return bscHold.Filter(node, ev);
        };

        TDataChecker checker(env);
        checker.Write(groupId, nodeId);
        bscHold.Start();
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId, nodeId);
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        env.Sim(TDuration::Seconds(10)); // let the node know the cluster state
        UNIT_ASSERT_EQUAL(GetPart(env, groupId, 1).GetStage(), NKikimrBridge::TGroupState::SYNCED); // not reset by BSC

        const TLogoBlobID id = std::get<1>(checker.GetWritten().back());
        auto query = [&] {
            const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                // not decommitted group is assimilated as BSC syncers do it
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvAssimilate(std::nullopt, std::nullopt, std::nullopt,
                    true, false));
            });
            auto assimilate = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvAssimilateResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(assimilate->Get()->Status, NKikimrProto::OK, assimilate->Get()->ErrorReason);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCheckIntegrity(id, TInstant::Max(),
                    NKikimrBlobStorage::FastRead));
            });
            auto integrity = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(integrity->Get()->Status, NKikimrProto::OK, integrity->Get()->ErrorReason);
        };
        query();
        UNIT_ASSERT_VALUES_EQUAL(numSentToPile1, 0);

        // BSC learns of the cluster state, the pile gets synced and takes the requests then
        bscHold.Release();
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Sim(TDuration::Seconds(10));
        query();
        UNIT_ASSERT(numSentToPile1);
    }
}
