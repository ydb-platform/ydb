#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

using namespace NBridgeTest;

namespace {

    // switch BridgeMode of the storage pool of its current generation
    NKikimrBlobStorage::TConfigResponse ChangeStoragePoolBridgeMode(TEnvironmentSetup& env, ui64 storagePoolId,
            bool bridgeMode) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableChangeStoragePoolBridgeMode();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(storagePoolId);
        cmd->SetBridgeMode(bridgeMode);
        cmd->SetItemConfigGeneration(ReadStoragePool(env, storagePoolId).GetItemConfigGeneration());
        return env.Invoke(request);
    }

    // two piles of 9 nodes, each pile being a datacenter of its own; non-uniform storage is enabled
    TEnvironmentSetup::TSettings MakeTwoPileSettings() {
        return {
            .NodeCount = 18,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .PrepareRuntime = [](TTestActorSystem& runtime) {
                runtime.GetAppDataBridgeConfig().SetEnableNonUniformStorage(true);
            },
            .LocationGenerator = [](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation location;
                const ui32 pile = (nodeId - 1) / 9 + 1;
                location.SetDataCenter(TStringBuilder() << "dc_" << pile);
                location.SetBridgePileName(TStringBuilder() << "pile_" << pile);
                location.SetRack(TStringBuilder() << "rack_" << nodeId);
                location.SetUnit(TStringBuilder() << "unit_" << nodeId);
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    constexpr ui32 FirstNodeOfPile[] = {1, 10};

    // ordinary pool with the settings of pool 1, its groups reside in the datacenter of the designated pile
    void CreateOrdinaryPool(TEnvironmentSetup& env, ui64 storagePoolId, ui32 numGroups, ui32 pileIndex) {
        auto pool = ReadStoragePool(env, 1);
        pool.SetStoragePoolId(storagePoolId);
        pool.SetName(TStringBuilder() << "pool_" << storagePoolId);
        pool.SetKind(pool.GetName());
        pool.ClearItemConfigGeneration();
        pool.SetBridgeMode(false);
        pool.SetNumGroups(numGroups);
        UNIT_ASSERT(pool.PDiskFilterSize());
        pool.MutablePDiskFilter(0)->AddProperty()->MutableLocation()->SetDataCenter(TStringBuilder() << "dc_"
            << pileIndex + 1);
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
    }

    // let the pool use PDisks of any datacenter
    void DropLocationFilter(TEnvironmentSetup& env, ui64 storagePoolId) {
        auto pool = ReadStoragePool(env, storagePoolId);
        for (auto& filter : *pool.MutablePDiskFilter()) {
            auto *properties = filter.MutableProperty();
            for (int i = properties->size() - 1; i >= 0; --i) {
                if (properties->Get(i).HasLocation()) {
                    properties->DeleteSubrange(i, 1);
                }
            }
        }
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    // make the pool placed in the single datacenter bridged one: this fails, as its parts can't be placed in other
    // piles, and nothing changes; then the pool is allowed to use PDisks of any datacenter
    void MakeLocatedPoolBridged(TEnvironmentSetup& env, ui64 storagePoolId, const std::vector<ui32>& groups) {
        auto response = ChangeStoragePoolBridgeMode(env, storagePoolId, true);
        UNIT_ASSERT_C(!response.GetSuccess(), "parts can't be placed outside the datacenter of the pool");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "Group fit error");
        UNIT_ASSERT(!ReadStoragePool(env, storagePoolId).GetBridgeMode());
        for (ui32 groupId : groups) {
            UNIT_ASSERT(!GetGroupInfo(env, groupId).HasBridgeGroupState());
        }
        DropLocationFilter(env, storagePoolId);
        response = ChangeStoragePoolBridgeMode(env, storagePoolId, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    // groups of the pool as tablets see them: ordinary groups, inline piles and bridge proxies, but not other parts
    std::vector<ui32> GetLogicalPoolGroups(TEnvironmentSetup& env, ui64 storagePoolId) {
        std::vector<ui32> res;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto *group : GetPoolGroups(baseConfig, storagePoolId)) {
            const auto info = GetGroupInfo(env, group->GetGroupId());
            if (!info.HasBridgePileId() || info.GetBridgeProxyGroupId() == group->GetGroupId()) {
                res.push_back(group->GetGroupId());
            }
        }
        return res;
    }

    bool HasGroup(const NKikimrBlobStorage::TBaseConfig& baseConfig, ui32 groupId) {
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetGroupId() == groupId) {
                return true;
            }
        }
        return false;
    }

    // the group has synced parts in both piles and relies on none of them in particular
    bool IsFullyBridged(const NKikimrBlobStorage::TGroupInfo& info) {
        const auto& state = info.GetBridgeGroupState();
        size_t numSynced = 0;
        NBridge::ForEachGroupPile(state, [&](TBridgePileId, const NKikimrBridge::TGroupState::TPile& pile) {
            numSynced += pile.GetStage() == NKikimrBridge::TGroupState::SYNCED;
        });
        return !state.GetAuthorityPileId() && numSynced == 2;
    }

    // the group has the only part, which is in the designated pile
    bool HasOnlyPart(TEnvironmentSetup& env, const NKikimrBlobStorage::TGroupInfo& info, ui32 pileIndex) {
        const auto& state = info.GetBridgeGroupState();
        size_t numParts = 0;
        NBridge::ForEachGroupPile(state, [&](TBridgePileId, const NKikimrBridge::TGroupState::TPile&) { ++numParts; });
        const auto *pile = NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(pileIndex));
        if (numParts != 1 || !pile || state.GetAuthorityPileId()) {
            return false;
        }
        // VDisks of the part reside in the pile
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == pile->GetGroupId()) {
                const ui32 nodeId = vslot.GetVSlotId().GetNodeId();
                UNIT_ASSERT(FirstNodeOfPile[pileIndex] <= nodeId && nodeId < FirstNodeOfPile[pileIndex] + 9);
            }
        }
        return true;
    }

    // wait until the node warden of the node knows the pile state
    void WaitForLocalPileState(TEnvironmentSetup& env, ui32 nodeId, ui32 pileIndex,
            NKikimrBridge::TClusterState::EPileState state) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge,
            new TEvNodeWardenQueryStorageConfig(true)), nodeId);
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        for (;;) {
            auto response = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(edge, false, deadline);
            UNIT_ASSERT_C(response, "timed out waiting for the local pile state");
            if (response->Get()->Config->GetClusterState().GetPerPileState(pileIndex) == state) {
                break;
            }
        }
        env.Runtime->Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, MakeBlobStorageNodeWardenID(nodeId), edge,
            nullptr, 0), nodeId);
    }

    NKikimrProto::EReplyStatus TryPut(TEnvironmentSetup& env, ui32 groupId, ui32 nodeId, const TLogoBlobID& id,
            const TString& data) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data,
                env.Runtime->GetClock() + TDuration::Seconds(30)));
        });
        return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status;
    }

}

Y_UNIT_TEST_SUITE(BridgePoolMode) {

    // ordinary groups residing in single piles become inline piles, get their parts in the other pile, which are synced
    // while the groups are written to, and then the groups don't rely on their original parts anymore
    Y_UNIT_TEST(MakeBridgedInTwoPileCluster) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 2, 1);
        TDataChecker checker(env);
        const auto groups = GetLogicalPoolGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        for (ui32 groupId : groups) {
            UNIT_ASSERT(!GetGroupInfo(env, groupId).HasBridgeGroupState());
            checker.Write(groupId);
        }

        MakeLocatedPoolBridged(env, 2, groups);
        for (ui32 groupId : groups) {
            const auto info = GetGroupInfo(env, groupId);
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeProxyGroupId(), groupId, info.ShortDebugString());
            const auto& state = info.GetBridgeGroupState();
            UNIT_ASSERT_C(TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId) ==
                TBridgePileId::FromPileIndex(1), info.ShortDebugString());
            UNIT_ASSERT_C(NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(0)), info.ShortDebugString());
        }

        for (ui32 i = 0; i < 3; ++i) {
            for (ui32 groupId : groups) {
                checker.Write(groupId, FirstNodeOfPile[i % 2]);
            }
            checker.CheckAll();
            env.Sim(TDuration::Seconds(1));
        }
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return IsFullyBridged(GetGroupInfo(env, groupId));
            });
        }, "groups fully bridged");

        for (ui32 groupId : groups) {
            checker.Write(groupId);
        }
        checker.CheckAll();
        for (ui32 pileIndex : {0, 1}) {
            CheckPileData(env, checker, pileIndex, FirstNodeOfPile[pileIndex]);
        }
        for (ui32 nodeId : {2u, 11u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.CheckAll(nodeId);
        }
    }

    // the group being made bridged one relies on its original part solely: when the pile of the part goes out, the
    // group is not written to (the new part already getting writes would take them alone otherwise), and the part keeps
    // its synced state when the pile gets back, so that the new part gets synced from it afterwards
    Y_UNIT_TEST(OriginalPileOutWhileMakingBridged) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 1, 1);
        const auto groups = GetLogicalPoolGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();
        const ui32 bridgedGroupId = GetLogicalPoolGroups(env, 1).front();
        TDataChecker checker(env);
        checker.Write(groupId);
        checker.Write(bridgedGroupId);

        // the new part is synced up to the stage when it gets every write, and no further
        NKikimrBlobStorage::TGroupInfo latestInfo; // as distributed by BSC
        bool hold = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                for (const auto& group : record.GetServiceSet().GetGroups()) {
                    if (group.GetGroupID() == groupId &&
                            latestInfo.GetGroupGeneration() <= group.GetGroupGeneration()) {
                        latestInfo.CopyFrom(group);
                    }
                }
            }
            if (hold && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                const auto *msg = ev->Get<NStorage::TEvNodeWardenNotifySyncerFinished>();
                if (msg->BridgeProxyGroupId.GetRawId() == groupId) {
                    const auto *pile = NBridge::FindGroupPileByGroupId(latestInfo.GetBridgeGroupState(),
                        msg->TargetGroupId);
                    if (pile && pile->GetStage() == NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA) {
                        held.emplace_back(nodeId, std::move(ev));
                        return false;
                    }
                }
            }
            return true;
        };

        MakeLocatedPoolBridged(env, 2, groups);
        WaitUntil(env, [&] { return !held.empty(); }, "new part synced up to data stage", TDuration::Seconds(1), 120);
        checker.Write(groupId);
        checker.CheckAll();

        // the pile of the original part goes out
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitForLocalPileState(env, 1, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        const TString data = "data written while the original part is out";
        const TLogoBlobID id(100501, 1, 1, 0, data.size(), 0);
        UNIT_ASSERT_UNEQUAL(TryPut(env, groupId, 1, id, data), NKikimrProto::OK);
        checker.Write(bridgedGroupId); // the bridged group keeps working

        // the pile gets back, the part it has is still synced
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        {
            const auto info = GetGroupInfo(env, groupId);
            const auto& state = info.GetBridgeGroupState();
            const auto *pile = NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT_C(pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED, info.ShortDebugString());
            UNIT_ASSERT_C(TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId) ==
                TBridgePileId::FromPileIndex(1), info.ShortDebugString());
        }
        checker.Write(groupId);
        checker.CheckAll();

        // the new part gets synced
        hold = false;
        for (auto& [nodeId, ev] : held) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        WaitUntil(env, [&] { return IsFullyBridged(GetGroupInfo(env, groupId)); }, "group fully bridged");
        checker.Write(groupId);
        checker.CheckAll();
        for (ui32 pileIndex : {0, 1}) {
            CheckPileData(env, checker, pileIndex, FirstNodeOfPile[pileIndex]);
        }
    }

    // bridged groups keep their ids and their parts in the primary pile, other parts are deleted while the groups are
    // written to; new groups are ordinary ones; then the pool is made bridged again
    Y_UNIT_TEST(MakeOrdinaryAndBack) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));
        const auto groups = GetLogicalPoolGroups(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        TDataChecker checker(env);
        std::vector<ui32> droppedParts;
        for (ui32 groupId : groups) {
            checker.Write(groupId);
            droppedParts.push_back(GetPileGroups(env, groupId).at(1));
        }

        auto response = ChangeStoragePoolBridgeMode(env, 1, false);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT(!ReadStoragePool(env, 1).GetBridgeMode());
        for (ui32 groupId : groups) {
            const auto info = GetGroupInfo(env, groupId);
            const auto& state = info.GetBridgeGroupState();
            UNIT_ASSERT_C(TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId) ==
                TBridgePileId::FromPileIndex(0), info.ShortDebugString());
        }

        // writes keep succeeding while the parts go away
        for (ui32 i = 0; i < 15; ++i) {
            for (ui32 groupId : groups) {
                checker.Write(groupId, FirstNodeOfPile[i % 2]);
            }
            env.Sim(TDuration::Seconds(5));
        }
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return HasOnlyPart(env, GetGroupInfo(env, groupId), 0);
            });
        }, "groups with the only part");
        const auto baseConfig = env.FetchBaseConfig();
        for (ui32 groupId : droppedParts) {
            UNIT_ASSERT_C(!HasGroup(baseConfig, groupId), "part not deleted GroupId# " << groupId);
        }
        checker.CheckAll();

        // new group of the pool is ordinary one
        SetNumGroups(env, 1, 3);
        env.Sim(TDuration::Seconds(30));
        const auto allGroups = GetLogicalPoolGroups(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(allGroups.size(), 3);
        for (ui32 groupId : allGroups) {
            if (std::ranges::find(groups, groupId) == groups.end()) {
                UNIT_ASSERT(!GetGroupInfo(env, groupId).HasBridgeGroupState());
                checker.Write(groupId);
            }
        }
        for (ui32 nodeId : {2u, 11u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.CheckAll(nodeId);
        }

        // the pool becomes bridged again: groups get their parts in the other pile anew
        response = ChangeStoragePoolBridgeMode(env, 1, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        for (ui32 groupId : allGroups) {
            checker.Write(groupId);
        }
        WaitUntil(env, [&] {
            return std::ranges::all_of(allGroups, [&](ui32 groupId) {
                return IsFullyBridged(GetGroupInfo(env, groupId));
            });
        }, "groups fully bridged");
        checker.CheckAll();
        for (ui32 pileIndex : {0, 1}) {
            CheckPileData(env, checker, pileIndex, FirstNodeOfPile[pileIndex]);
        }
    }

    // the part dropped by the pool made ordinary stops getting requests for the whole delay before it is deleted, even
    // when the pool has been made bridged and ordinary again in between
    Y_UNIT_TEST(RepeatedModeChanges) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = GetLogicalPoolGroups(env, 1).front();
        const ui32 partId = GetPileGroups(env, groupId).at(1);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto change = [&](bool bridgeMode) {
            const auto response = ChangeStoragePoolBridgeMode(env, 1, bridgeMode);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        };
        auto getPartStage = [&] {
            const auto info = GetGroupInfo(env, groupId);
            const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT_C(pile, info.ShortDebugString());
            return pile->GetStage();
        };

        // the part stops getting requests, and the pool is made bridged again before the part is deleted
        change(false);
        WaitUntil(env, [&] { return getPartStage() == NKikimrBridge::TGroupState::BLOCKS; }, "part stopped",
            TDuration::Seconds(1), 30);
        change(true);
        WaitUntil(env, [&] { return IsFullyBridged(GetGroupInfo(env, groupId)); }, "group fully bridged");
        UNIT_ASSERT_VALUES_EQUAL(GetPileGroups(env, groupId).at(1), partId); // the same part has been synced
        env.Sim(TDuration::Seconds(40)); // longer than the delay since the part stopped for the first time
        checker.Write(groupId);

        // the part is dropped again: it is still there before the delay passes since it has stopped this time
        change(false);
        env.Sim(TDuration::Seconds(15));
        UNIT_ASSERT_C(HasGroup(env.FetchBaseConfig(), partId), "part deleted before the delay passed");
        checker.Write(groupId);
        checker.CheckAll();
        WaitUntil(env, [&] { return !HasGroup(env.FetchBaseConfig(), partId); }, "part deleted");
        UNIT_ASSERT(HasOnlyPart(env, GetGroupInfo(env, groupId), 0));
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the group with the only part in the pile can't lose it along with the pile, and the part keeps its data and state
    // when the pile gets back after being out
    Y_UNIT_TEST(OnlyPartPileOut) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = GetLogicalPoolGroups(env, 1).front();
        TDataChecker checker(env);
        checker.Write(groupId);

        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableChangeStoragePoolBridgeMode();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(1);
        cmd->SetBridgeMode(false);
        cmd->SetRetainPileName("pile_3");
        cmd->SetItemConfigGeneration(ReadStoragePool(env, 1).GetItemConfigGeneration());
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "unknown pile must be rejected");
        cmd->SetRetainPileName("pile_2");
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitUntil(env, [&] { return HasOnlyPart(env, GetGroupInfo(env, groupId), 1); }, "group with the only part");
        checker.Write(groupId);
        checker.CheckAll();

        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitForLocalPileState(env, 1, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        const TString data = "data written while the only part is out";
        UNIT_ASSERT_UNEQUAL(TryPut(env, groupId, 1, TLogoBlobID(100501, 1, 1, 0, data.size(), 0), data),
            NKikimrProto::OK);
        ExpectError(RemovePile(env, "pile_2"), "relies on its part in the pile solely");

        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT(HasOnlyPart(env, GetGroupInfo(env, groupId), 1));
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the pool kept ordinary at conversion is made bridged later: its groups become inline piles keeping their ids and
    // data, and new groups are bridged ones; the other ordinary pool stays as it is
    Y_UNIT_TEST(MakeBridgedInSinglePileCluster) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker, 1, true);
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        const auto poolGroups = GetPoolGroups(baseConfig, 2);
        UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), 1);
        const ui32 otherGroupId = poolGroups.front()->GetGroupId();
        for (ui32 id : {groupId, otherGroupId}) {
            checker.Write(id);
        }

        ui32 numInnerPuts = 0; // puts issued by the bridge proxy to the DS proxy of the inline pile
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                if (auto *msg = ev->Get<TEvBlobStorage::TEvPut>(); msg->ForceGroupGeneration &&
                        msg->Id.TabletID() == 100500) {
                    ++numInnerPuts;
                }
            }
            return true;
        };

        auto getGeneration = [&](ui32 id) {
            return FindGroup(env.FetchBaseConfig(), id).GetGroupGeneration();
        };
        const ui32 generation = getGeneration(groupId);
        const ui32 otherGeneration = getGeneration(otherGroupId);

        // the generation of the pool fences concurrent changes
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableChangeStoragePoolBridgeMode();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(1);
        cmd->SetBridgeMode(true);
        cmd->SetItemConfigGeneration(ReadStoragePool(env, 1).GetItemConfigGeneration() + 1);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "stale generation must be rejected");

        response = ChangeStoragePoolBridgeMode(env, 1, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(ReadStoragePool(env, 1).GetBridgeMode());
        UNIT_ASSERT(!ReadStoragePool(env, 2).GetBridgeMode());

        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeProxyGroupId(), groupId, info.ShortDebugString());
        UNIT_ASSERT_C(TBridgePileId::FromProto(&info, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId) ==
            TBridgePileId::FromPileIndex(0), info.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(info.GetGroupGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().PileSize(), 1, info.ShortDebugString());
        UNIT_ASSERT_EQUAL(info.GetBridgeGroupState().GetPile(0).GetStage(), NKikimrBridge::TGroupState::SYNCED);
        const auto otherInfo = GetGroupInfo(env, otherGroupId);
        UNIT_ASSERT_C(!otherInfo.HasBridgeGroupState(), otherInfo.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(otherInfo.GetGroupGeneration(), otherGeneration);

        // the same mode is accepted without changes
        response = ChangeStoragePoolBridgeMode(env, 1, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(getGeneration(groupId), generation + 1);

        // the pool is redefined with its new mode or without one, but its mode can't be changed that way
        auto pool = ReadStoragePool(env, 1);
        pool.SetBridgeMode(false);
        request.Clear();
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "mode change by redefinition must be rejected");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "can't change storage pool BridgeMode");
        pool.ClearBridgeMode();
        pool.SetNumGroups(2);
        request.Clear();
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
        std::optional<ui32> newGroupId;
        {
            const auto current = env.FetchBaseConfig();
            for (const auto *group : GetPoolGroups(current, 1)) {
                if (group->GetIsProxyGroup()) {
                    UNIT_ASSERT(!newGroupId);
                    newGroupId = group->GetGroupId();
                }
            }
        }
        UNIT_ASSERT(newGroupId);
        checker.Write(*newGroupId);

        auto checkAll = [&](ui32 nodeId) {
            const ui32 prev = numInnerPuts;
            checker.Write(groupId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(numInnerPuts, prev + 1);
            checker.Write(otherGroupId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(numInnerPuts, prev + 1);
            checker.CheckAll(nodeId);
        };
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            checkAll(nodeId);
        }

        // the converted group keeps working through disk reassignment and restarts
        ReassignOneDisk(env, groupId);
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, groupId).GetBridgeGroupState().GetPile(0).GetGroupGeneration(),
            getGeneration(groupId));
        checkAll(1);
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting node " << nodeId << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checkAll(nodeId);
        }
    }

    // BridgeMode of pools may be changed only in the cluster supporting both kinds of them
    Y_UNIT_TEST(RequiresNonUniformStorage) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        const auto response = ChangeStoragePoolBridgeMode(env, 1, false);
        UNIT_ASSERT_C(!response.GetSuccess(), "mode change must be rejected");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "enable_non_uniform_storage");
        checker.Write(groupId);
        checker.CheckAll();
    }

}
