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

    TEnvironmentSetup::TSettings MakeTwoPileSettings(TBlobStorageGroupType::EErasureSpecies erasure =
            TBlobStorageGroupType::Erasure4Plus2Block) {
        return {
            .NodeCount = 16,
            .Erasure = erasure,
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

    // the re-run of rejoin while the pile still waits for its static groups leaves the parts of dynamic groups at the
    // SYNCED stage of their previous sync; the pile becomes SYNCHRONIZED, but the parts miss the writes made while it
    // was NOT_SYNCHRONIZED_1 the second time
    Y_UNIT_TEST(RejoinReRunDuringNotSynchronized2) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        constexpr ui32 nodeId = 2; // node of the pile staying connected

        TDataChecker checker(env);
        checker.Write(groupId, nodeId);

        // hold the commits finishing the sync of the static group parts of the pile, so the pile stays
        // NOT_SYNCHRONIZED_2 after BSC has synced the dynamic group parts and cleared UnsyncedBSC
        std::vector<std::pair<std::unique_ptr<IEventHandle>, ui32>> heldStaticCommits;
        bool holdingStaticCommits = true;

        // hold the storage configs reported to the warden of the writing node once it has learned the
        // NOT_SYNCHRONIZED_1 state of the pile: the node keeps skipping the pile in its writes while the pile switches
        // back to NOT_SYNCHRONIZED_2 and its dynamic group parts keep the SYNCED stage of their previous sync
        std::vector<std::pair<std::unique_ptr<IEventHandle>, ui32>> heldConfigs;
        bool holdingConfigs = false;

        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (holdingStaticCommits && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (record.HasUpdateBridgeGroupInfo()) {
                    const auto& cmd = record.GetUpdateBridgeGroupInfo();
                    const auto *pile = NBridge::GetGroupPile(cmd.GetBridgeGroupInfo().GetBridgeGroupState(),
                        TBridgePileId::FromPileIndex(1));
                    if (pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                        heldStaticCommits.emplace_back(std::move(ev), node);
                        return false;
                    }
                }
            } else if (holdingConfigs && node == nodeId &&
                    ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                const auto& msg = *ev->Get<TEvNodeWardenStorageConfig>();
                if (msg.Config && msg.Config->HasClusterState() &&
                        msg.Config->GetClusterState().GetPerPileState(1) !=
                            NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1) {
                    heldConfigs.emplace_back(std::move(ev), node);
                    return false;
                }
            }
            return true;
        };

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId, nodeId); // reaches pile 0 only

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1); // rejoin
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        checker.Write(groupId, nodeId); // reaches both piles, the part of pile 1 is resynced by BSC

        // wait until BSC has finished the dynamic group parts and reported it; the pile keeps waiting for its static
        // group part, whose final commit is held
        NKikimrBlobStorage::TStorageConfig config;
        WaitUntil(env, [&] {
            config = QueryStorageConfig(env);
            for (const auto& pss : config.GetClusterStateDetails().GetPileSyncState()) {
                if (TBridgePileId::FromProto(&pss,
                        &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId) ==
                        TBridgePileId::FromPileIndex(1)) {
                    return !pss.GetUnsyncedBSC();
                }
            }
            return false;
        }, "BSC report of the pile sync", [&] { return config.GetClusterStateDetails().DebugString(); });
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);

        // rejoin is re-run: the pile gets NOT_SYNCHRONIZED_1, which resets nothing, and switches back to
        // NOT_SYNCHRONIZED_2 right away, which does not reset the dynamic group parts either, as BSC has already
        // reported them; the warden of the node is held at NOT_SYNCHRONIZED_1, so the writes made meanwhile skip the
        // pile and their data never reaches its part
        holdingConfigs = true;
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1); // rejoin re-run
        env.Sim(TDuration::Seconds(10)); // let the node learn NOT_SYNCHRONIZED_1
        checker.Write(groupId, nodeId); // reaches pile 0 only
        checker.Write(groupId, nodeId);
        UNIT_ASSERT(!heldConfigs.empty());
        holdingConfigs = false;
        for (auto& [ev, node] : std::exchange(heldConfigs, {})) {
            env.Runtime->Send(new IEventHandle(ev->GetRecipientRewrite(), ev->Sender, ev->ReleaseBase().Release(), 0,
                ev->Cookie), node);
        }

        // release the held commits of the static group sync: the one of the current generation finishes it, and the
        // pile becomes SYNCHRONIZED
        UNIT_ASSERT(!heldStaticCommits.empty());
        holdingStaticCommits = false;
        for (auto& [ev, node] : std::exchange(heldStaticCommits, {})) {
            env.Runtime->Send(new IEventHandle(ev->GetRecipientRewrite(), ev->Sender, ev->ReleaseBase().Release(), 0,
                ev->Cookie), node);
        }
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = nullptr;

        // the part of the dynamic group has never been reset: it misses the writes made while the pile was
        // NOT_SYNCHRONIZED_1, so reading it directly must fail until it is resynced
        UNIT_ASSERT_EQUAL(GetPart(env, groupId, 1).GetStage(), NKikimrBridge::TGroupState::SYNCED);
        CheckPileData(env, checker, 1, 9);
    }

    // the forced evict of the VDisk of the erasure none group part puts an empty VDisk in its place; the part cannot
    // rebuild itself, but it keeps its SYNCED stage, so the pile stays SYNCHRONIZED without the data
    Y_UNIT_TEST(ForcedEvictWhileSynchronized) {
        TEnvironmentSetup env(MakeTwoPileSettings(TBlobStorageGroupType::ErasureNone));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        TDataChecker checker(env);
        checker.Write(groupId, 2); // reaches both piles
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::SYNCHRONIZED);

        // find the single VDisk of the pile's part
        const auto baseConfig = env.FetchBaseConfig();
        const NKikimrBlobStorage::TBaseConfig::TVSlot *target = nullptr;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == pileGroups[1]) {
                UNIT_ASSERT(!target);
                target = &vslot;
            }
        }
        UNIT_ASSERT(target);

        // evict it the forced way, like the viewer does with force=true: the failure model checks are off and donor
        // mode is suppressed, so the new VDisk is empty and the part has nothing to rebuild itself from
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreDegradedGroupsChecks(true);
        request.SetIgnoreDisintegratedGroupsChecks(true);
        request.SetIgnoreGroupFailModelChecks(true);
        auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
        reassign->SetGroupId(pileGroups[1]);
        reassign->SetGroupGeneration(target->GetGroupGeneration());
        reassign->SetFailRealmIdx(target->GetFailRealmIdx());
        reassign->SetFailDomainIdx(target->GetFailDomainIdx());
        reassign->SetVDiskIdx(target->GetVDiskIdx());
        reassign->SetSuppressDonorMode(true);
        const auto response = env.Invoke(request);

        if (!response.GetSuccess()) {
            Cerr << "*** reassign refused: " << response.GetErrorDescription() << Endl;
            return; // the reassign is refused: also fine
        }
        env.Sim(TDuration::Minutes(1)); // let the new empty VDisk replace the old one

        // the pile keeps its SYNCHRONIZED state, while its part of the group has no data anymore
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::SYNCHRONIZED);
        CheckPileData(env, checker, 1, 9);
    }
}
