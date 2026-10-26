#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/distconf.h>

#include <util/system/type_name.h>

#include <algorithm>
#include <array>

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

    // the part of the bridged group in the pile, as BSC knows it
    NKikimrBridge::TGroupState::TPile GetPart(TEnvironmentSetup& env, ui32 groupId, ui32 pileIndex) {
        const auto info = GetGroupInfo(env, groupId);
        const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(pileIndex));
        UNIT_ASSERT_C(pile, info.DebugString());
        return *pile;
    }

    // holds the commits finishing the sync of the static group parts of pile 1, so the pile stays NOT_SYNCHRONIZED_2
    // after BSC has synced the dynamic group parts and cleared UnsyncedBSC
    class TStaticSyncFinishHold {
        TEnvironmentSetup& Env;
        bool Holding = true;
        std::vector<std::pair<std::unique_ptr<IEventHandle>, ui32>> Held;

    public:
        explicit TStaticSyncFinishHold(TEnvironmentSetup& env)
            : Env(env)
        {}

        bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (Holding && ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (record.HasUpdateBridgeGroupInfo()) {
                    const auto& cmd = record.GetUpdateBridgeGroupInfo();
                    const auto *pile = NBridge::GetGroupPile(cmd.GetBridgeGroupInfo().GetBridgeGroupState(),
                        TBridgePileId::FromPileIndex(1));
                    if (pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                        Held.emplace_back(std::move(ev), nodeId);
                        return false;
                    }
                }
            }
            return true;
        }

        void Release() {
            UNIT_ASSERT(!Held.empty());
            Holding = false;
            for (auto& [ev, nodeId] : std::exchange(Held, {})) {
                Env.Runtime->Send(new IEventHandle(ev->GetRecipientRewrite(), ev->Sender, ev->ReleaseBase().Release(), 0,
                    ev->Cookie), nodeId);
            }
        }
    };

    // take pile 1 out, write, and rejoin it; returns when BSC has synced the dynamic group parts of the pile and
    // reported it, while the pile stays NOT_SYNCHRONIZED_2 waiting for its static group parts held by the filter
    void RejoinUntilDynamicPartsSynced(TEnvironmentSetup& env, TDataChecker& checker, ui32 groupId, ui32 nodeId) {
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId, nodeId); // reaches pile 0 only

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1); // rejoin
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        checker.Write(groupId, nodeId); // reaches both piles, the part of pile 1 is resynced by BSC

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
        UNIT_ASSERT_EQUAL(GetPart(env, groupId, 1).GetStage(), NKikimrBridge::TGroupState::SYNCED);
    }

    // VDisks of the group, as BSC knows them
    std::vector<NKikimrBlobStorage::TBaseConfig::TVSlot> GetGroupVSlots(TEnvironmentSetup& env, ui32 groupId) {
        std::vector<NKikimrBlobStorage::TBaseConfig::TVSlot> res;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == groupId) {
                res.push_back(vslot);
            }
        }
        UNIT_ASSERT(!res.empty());
        return res;
    }

    // the request with the checks of the failure model off, like the viewer's evict with force=true makes
    NKikimrBlobStorage::TConfigRequest MakeForcedRequest() {
        NKikimrBlobStorage::TConfigRequest request;
        request.SetIgnoreDegradedGroupsChecks(true);
        request.SetIgnoreDisintegratedGroupsChecks(true);
        request.SetIgnoreGroupFailModelChecks(true);
        return request;
    }

    // the evict with donor mode suppressed, so the new VDisk is empty and has nothing to restore its data from but the
    // other VDisks of the group
    void AddForcedReassign(NKikimrBlobStorage::TConfigRequest& request,
            const NKikimrBlobStorage::TBaseConfig::TVSlot& vslot) {
        auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
        reassign->SetGroupId(vslot.GetGroupId());
        reassign->SetGroupGeneration(vslot.GetGroupGeneration());
        reassign->SetFailRealmIdx(vslot.GetFailRealmIdx());
        reassign->SetFailDomainIdx(vslot.GetFailDomainIdx());
        reassign->SetVDiskIdx(vslot.GetVDiskIdx());
        reassign->SetSuppressDonorMode(true);
    }

    void CheckRefusedAsDataLoss(const NKikimrBlobStorage::TConfigResponse& response) {
        UNIT_ASSERT_C(!response.GetSuccess(), "the change losing the data of the part is accepted");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "unable to restore its data from its pile");
    }

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

        TStaticSyncFinishHold staticHold(env);

        // when the proxy of the group on the writing node learns the NOT_SYNCHRONIZED_1 state of the pile, a write
        // is issued right away: the write lands between the rejoin re-run and the switch of the pile back to
        // NOT_SYNCHRONIZED_2, like a write made while the nodes of the pile are down, so it skips the pile; the proxy
        // is the only bridge proxy of the node, as nothing else uses the bridged groups from it
        const TActorId putEdge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        bool gapWriteArmed = false;
        bool gapWriteIssued = false;

        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (!staticHold.Filter(node, ev)) {
                return false;
            } else if (gapWriteArmed && !gapWriteIssued && node == nodeId &&
                    ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                const auto& msg = *ev->Get<TEvNodeWardenStorageConfig>();
                if (msg.Config && msg.Config->HasClusterState() &&
                        msg.Config->GetClusterState().GetPerPileState(1) ==
                            NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1) {
                    if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                            TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                        gapWriteIssued = true;
                        checker.SendPut(groupId, putEdge);
                    }
                }
            }
            return true;
        };

        RejoinUntilDynamicPartsSynced(env, checker, groupId, nodeId);

        // rejoin is re-run: the pile gets NOT_SYNCHRONIZED_1 and switches back to NOT_SYNCHRONIZED_2 right away;
        // the write issued while the proxy of the node knows the pile as NOT_SYNCHRONIZED_1 skips the pile, and its
        // data never reaches the part
        gapWriteArmed = true;
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1); // rejoin re-run
        const auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(putEdge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(put->Get()->Status, NKikimrProto::OK, put->Get()->ToString());
        UNIT_ASSERT(gapWriteIssued);

        // release the held commits of the static group sync: the one of the current generation finishes it, and the
        // pile becomes SYNCHRONIZED
        staticHold.Release();
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = nullptr;

        // the part of the dynamic group in the pile must hold every acknowledged write when the pile reports
        // SYNCHRONIZED, including the one made while it was NOT_SYNCHRONIZED_1
        UNIT_ASSERT_EQUAL(GetPart(env, groupId, 1).GetStage(), NKikimrBridge::TGroupState::SYNCED);
        CheckPileData(env, checker, 1, 9);
    }

    // the forced evict of the VDisk of the erasure none group part puts an empty VDisk in its place; the part cannot
    // restore its data from its pile, but it would keep its SYNCED stage, and the pile its SYNCHRONIZED state, so the
    // evict is refused; taking the pile out lets it go, and the pile gets the data back on rejoin
    Y_UNIT_TEST(ForcedEvictWhileSynchronized) {
        TEnvironmentSetup env(MakeTwoPileSettings(TBlobStorageGroupType::ErasureNone));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        TDataChecker checker(env);
        checker.Write(groupId, 2); // reaches both piles
        UNIT_ASSERT_EQUAL(GetPileState(env, 1), NKikimrBridge::TClusterState::SYNCHRONIZED);

        auto vslots = GetGroupVSlots(env, pileGroups[1]);
        UNIT_ASSERT_VALUES_EQUAL(vslots.size(), 1);
        auto request = MakeForcedRequest();
        AddForcedReassign(request, vslots.front());
        CheckRefusedAsDataLoss(env.Invoke(request));

        // the pile out of data quorum gets its parts reset and resynced when it gets back
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId, 2); // reaches pile 0 only
        vslots = GetGroupVSlots(env, pileGroups[1]); // the reset of the part bumps the generation of its group
        request = MakeForcedRequest();
        AddForcedReassign(request, vslots.front());
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Minutes(1)); // let the new empty VDisk replace the old one

        RejoinPile(env, 1);
        checker.CheckAll(2);
        CheckPileData(env, checker, 1, 9);
    }

    // the same while the pile is NOT_SYNCHRONIZED_2 waiting for its static groups: the part already synced keeps its
    // SYNCED stage, and the pile gets SYNCHRONIZED without the data
    Y_UNIT_TEST(ForcedEvictWhileNotSynchronized2) {
        TEnvironmentSetup env(MakeTwoPileSettings(TBlobStorageGroupType::ErasureNone));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        constexpr ui32 nodeId = 2; // node of the pile staying connected
        TDataChecker checker(env);
        checker.Write(groupId, nodeId);

        TStaticSyncFinishHold staticHold(env);
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            return staticHold.Filter(node, ev);
        };
        RejoinUntilDynamicPartsSynced(env, checker, groupId, nodeId);

        const auto vslots = GetGroupVSlots(env, pileGroups[1]);
        UNIT_ASSERT_VALUES_EQUAL(vslots.size(), 1);
        auto request = MakeForcedRequest();
        AddForcedReassign(request, vslots.front());
        CheckRefusedAsDataLoss(env.Invoke(request));

        staticHold.Release();
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = nullptr;
        CheckPileData(env, checker, 1, 9);
    }

    // the wipe of the VDisk of the erasure none group part empties the part in place
    Y_UNIT_TEST(ForcedWipeWhileSynchronized) {
        TEnvironmentSetup env(MakeTwoPileSettings(TBlobStorageGroupType::ErasureNone));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        TDataChecker checker(env);
        checker.Write(groupId, 2);

        const auto vslots = GetGroupVSlots(env, pileGroups[1]);
        UNIT_ASSERT_VALUES_EQUAL(vslots.size(), 1);
        const auto& vslot = vslots.front();
        auto request = MakeForcedRequest();
        auto *wipe = request.AddCommand()->MutableWipeVDisk();
        wipe->MutableVSlotId()->CopyFrom(vslot.GetVSlotId());
        VDiskIDFromVDiskID(TVDiskID(TGroupId::FromValue(vslot.GetGroupId()), vslot.GetGroupGeneration(),
            vslot.GetFailRealmIdx(), vslot.GetFailDomainIdx(), vslot.GetVDiskIdx()), wipe->MutableVDiskId());
        CheckRefusedAsDataLoss(env.Invoke(request));

        env.Sim(TDuration::Minutes(1));
        CheckPileData(env, checker, 1, 9);
    }

    // the BROKEN drive of the VDisk of the erasure none group part gets the VDisk replaced by an empty one
    Y_UNIT_TEST(ForcedBrokenDriveWhileSynchronized) {
        TEnvironmentSetup env(MakeTwoPileSettings(TBlobStorageGroupType::ErasureNone));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        TDataChecker checker(env);
        checker.Write(groupId, 2);

        const auto vslots = GetGroupVSlots(env, pileGroups[1]);
        UNIT_ASSERT_VALUES_EQUAL(vslots.size(), 1);
        const auto& vslot = vslots.front();
        auto request = MakeForcedRequest();
        auto *ds = request.AddCommand()->MutableUpdateDriveStatus();
        ds->MutableHostKey()->SetNodeId(vslot.GetVSlotId().GetNodeId());
        ds->SetPDiskId(vslot.GetVSlotId().GetPDiskId());
        ds->SetStatus(NKikimrBlobStorage::EDriveStatus::BROKEN);
        CheckRefusedAsDataLoss(env.Invoke(request));

        env.Sim(TDuration::Minutes(1));
        CheckPileData(env, checker, 1, 9);
    }

    // the VDisks of the block-4-2 group part not ready count along with the evicted one: each evict alone is fine, but
    // the one made while two other VDisks are down leaves the part with less than its data needs
    Y_UNIT_TEST(ForcedEvictWithVDisksNotReadyWhileSynchronized) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(pileGroups.size(), 2);

        TDataChecker checker(env);
        checker.Write(groupId, 2);
        env.Sim(TDuration::Minutes(1)); // let all the VDisks be ready

        // the VDisks of the part, the one on the node reading the data of the pile in the end goes first
        constexpr ui32 readerNodeId = 9;
        auto vslots = GetGroupVSlots(env, pileGroups[1]);
        UNIT_ASSERT_VALUES_EQUAL(vslots.size(), 8);
        std::ranges::partition(vslots, [&](const auto& vslot) { return vslot.GetVSlotId().GetNodeId() == readerNodeId; });
        UNIT_ASSERT_VALUES_EQUAL(vslots.front().GetVSlotId().GetNodeId(), readerNodeId);

        // the single one is restored from the other VDisks of the part, so it passes; the part takes every node of the
        // pile with its only PDisk, so there is no place for the new VDisk though
        auto request = MakeForcedRequest();
        AddForcedReassign(request, vslots.front());
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "the pile is expected to have no place for the new VDisk");
        UNIT_ASSERT_C(!response.GetErrorDescription().Contains("unable to restore its data from its pile"),
            response.GetErrorDescription());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "failed to allocate group");

        // two other VDisks go down
        const std::array stoppedNodeIds{vslots[1].GetVSlotId().GetNodeId(), vslots[2].GetVSlotId().GetNodeId()};
        for (const ui32 nodeId : stoppedNodeIds) {
            env.StopNode(nodeId);
        }
        env.Sim(TDuration::Seconds(30)); // let BSC see them not ready

        request = MakeForcedRequest();
        AddForcedReassign(request, vslots.front());
        CheckRefusedAsDataLoss(env.Invoke(request));

        for (const ui32 nodeId : stoppedNodeIds) {
            env.StartNode(nodeId);
        }
        env.Sim(TDuration::Minutes(1));
        CheckPileData(env, checker, 1, readerNodeId);
    }
}
