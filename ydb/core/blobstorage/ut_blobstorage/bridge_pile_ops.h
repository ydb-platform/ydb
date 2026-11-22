#pragma once

#include "bridge_config_yaml.h"

// helpers for bridge tests adding piles to the cluster and removing them
namespace NBridgeTest {

    // notification of distconf by BSC that it has performed its part of the pending pile operation
    inline const NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot *GetBridgePileOperationNotification(IEventHandle& ev) {
        if (ev.GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
            const auto& record = ev.Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
            if (record.HasNotifyBridgePileOperation()) {
                return &record;
            }
        }
        return nullptr;
    }

    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult AddPile(TEnvironmentSetup& env, const TString& name) {
        return InvokeDistconf(env, [&](auto& record) { record.MutableAddBridgePile()->SetPileName(name); });
    }

    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult RemovePile(TEnvironmentSetup& env, const TString& name,
            ui32 nodeId = 1) {
        return InvokeDistconf(env, [&](auto& record) { record.MutableRemoveBridgePile()->SetPileName(name); }, nodeId);
    }

    // start nodes of the pile with BridgeConfig listing it
    inline void StartPileNodes(TEnvironmentSetup& env, const TString& name, ui32 firstNodeId, ui32 lastNodeId) {
        env.Runtime->GetAppDataBridgeConfig().AddPiles()->SetName(name);
        env.ConfiguredNodeCount = lastNodeId;
        for (ui32 nodeId = firstNodeId; nodeId <= lastNodeId; ++nodeId) {
            env.StartNode(nodeId);
        }
        env.Sim(TDuration::Seconds(30));
    }

    // wait until BSC performs its part of the pending pile operation
    inline NKikimrBlobStorage::TStorageConfig WaitPileOperationFinished(TEnvironmentSetup& env, ui32 nodeId = 1) {
        NKikimrBlobStorage::TStorageConfig config;
        WaitUntil(env, [&] {
            config = QueryStorageConfig(env, nodeId);
            return !config.GetClusterStateDetails().HasPendingPileOperation();
        }, "pile operation finish", [&] { return config.GetClusterStateDetails().DebugString(); });
        return config;
    }

    inline void AddPileAndWait(TEnvironmentSetup& env, const TString& name) {
        const auto res = AddPile(env, name);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        WaitPileOperationFinished(env);
    }

    inline void RemovePileAndWait(TEnvironmentSetup& env, const TString& name) {
        const auto res = RemovePile(env, name);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        WaitPileOperationFinished(env);
    }

    inline void ExpectError(const NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& res, const TString& error) {
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
            res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), error);
    }

    inline void RejoinPile(TEnvironmentSetup& env, ui32 pileIndex) {
        SwitchPileState(env, pileIndex, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, pileIndex, NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageConfig(env).GetClusterStateDetails().PileSyncStateSize(), 0);
    }

    // the pile has its part in every bridged group, static and dynamic ones, placed on its nodes, and its state
    // storage replicas; returns the number of dynamic bridged groups
    inline size_t CheckPileParts(TEnvironmentSetup& env, ui32 pileIndex, ui32 firstNodeId, ui32 lastNodeId) {
        const auto pileId = TBridgePileId::FromPileIndex(pileIndex);
        auto inPile = [&](ui32 nodeId) { return firstNodeId <= nodeId && nodeId <= lastNodeId; };
        const auto config = QueryStorageConfig(env);

        const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
        THashMap<ui32, const NKikimrBlobStorage::TGroupInfo*> staticGroups;
        for (const auto& group : ss.GetGroups()) {
            staticGroups.emplace(group.GetGroupID(), &group);
        }
        THashSet<ui32> pileGroupIds; // parts of bridged groups, static and dynamic ones, in the pile
        for (const auto& group : ss.GetGroups()) {
            if (!group.HasBridgeGroupState()) {
                continue;
            }
            const auto *pile = NBridge::GetGroupPile(group.GetBridgeGroupState(), pileId);
            UNIT_ASSERT_C(pile, group.DebugString());
            const auto it = staticGroups.find(pile->GetGroupId());
            UNIT_ASSERT(it != staticGroups.end());
            for (const auto& realm : it->second->GetRings()) {
                for (const auto& domain : realm.GetFailDomains()) {
                    for (const auto& location : domain.GetVDiskLocations()) {
                        UNIT_ASSERT_C(inPile(location.GetNodeID()), it->second->DebugString());
                    }
                }
            }
            UNIT_ASSERT(pileGroupIds.insert(pile->GetGroupId()).second);
        }
        UNIT_ASSERT(!pileGroupIds.empty());

        for (const auto *pb : {&config.GetStateStorageConfig(), &config.GetStateStorageBoardConfig(),
                &config.GetSchemeBoardConfig()}) {
            size_t numRingGroups = 0;
            for (const auto& ringGroup : pb->GetRingGroups()) {
                if (TBridgePileId::FromProto(&ringGroup, &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId) ==
                        pileId) {
                    ++numRingGroups;
                    for (const auto& ring : ringGroup.GetRing()) {
                        for (const ui32 nodeId : ring.GetNode()) {
                            UNIT_ASSERT(inPile(nodeId));
                        }
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL_C(numRingGroups, 1, pb->DebugString());
        }

        // every dynamic bridged group has exactly one part in the pile, and only parts in the pile reside on its nodes
        const auto baseConfig = env.FetchBaseConfig();

        // static groups reside on PDisks of BSC, as there can be only one PDisk on the drive
        THashMap<std::tuple<ui32, ui32>, const NKikimrBlobStorage::TBaseConfig::TPDisk*> bscPDisks;
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            bscPDisks.emplace(std::make_tuple(pdisk.GetNodeId(), pdisk.GetPDiskId()), &pdisk);
        }
        for (const auto& pdisk : ss.GetPDisks()) {
            if (inPile(pdisk.GetNodeID())) {
                const auto it = bscPDisks.find(std::make_tuple(pdisk.GetNodeID(), pdisk.GetPDiskID()));
                UNIT_ASSERT_C(it != bscPDisks.end(), pdisk.DebugString());
                UNIT_ASSERT_VALUES_EQUAL(it->second->GetPath(), pdisk.GetPath());
                UNIT_ASSERT_VALUES_EQUAL(it->second->GetGuid(), pdisk.GetPDiskGuid());
            }
        }

        THashSet<ui32> existingGroupIds;
        for (const auto& group : baseConfig.GetGroup()) {
            existingGroupIds.insert(group.GetGroupId());
        }
        THashMap<ui32, ui32> vslotNodes;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (existingGroupIds.contains(vslot.GetGroupId())) { // not the one destroyed along with removed pile
                vslotNodes.emplace(vslot.GetGroupId(), vslot.GetVSlotId().GetNodeId());
            }
        }
        size_t numBridged = 0;
        for (const auto& group : baseConfig.GetGroup()) {
            if (staticGroups.contains(group.GetGroupId())) {
                continue; // checked above
            }
            const auto info = GetGroupInfo(env, group.GetGroupId());
            if (!info.HasBridgeGroupState()) {
                continue;
            }
            ++numBridged;
            const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), pileId);
            UNIT_ASSERT_C(pile, info.DebugString());
            UNIT_ASSERT(pileGroupIds.insert(pile->GetGroupId()).second);
        }
        for (const auto& [groupId, nodeId] : vslotNodes) {
            UNIT_ASSERT_VALUES_EQUAL_C(pileGroupIds.contains(groupId), inPile(nodeId), "GroupId# " << groupId);
        }
        return numBridged;
    }

    // the pile has been removed from the cluster: it is INACTIVE, it has no parts in bridged groups, static and dynamic
    // ones, no VDisks other than destroyed static ones, no state storage replicas and no sync state; the lists of piles
    // of bridged groups have no absent entries at the end
    inline void CheckPileGone(TEnvironmentSetup& env, ui32 pileIndex, ui32 firstNodeId, ui32 lastNodeId,
            ui32 queryNodeId = 1) {
        const auto pileId = TBridgePileId::FromPileIndex(pileIndex);
        auto inPile = [&](ui32 nodeId) { return firstNodeId <= nodeId && nodeId <= lastNodeId; };
        const auto config = QueryStorageConfig(env, queryNodeId);
        UNIT_ASSERT_EQUAL(NBridge::GetPileState(config.GetClusterState(), pileId), NKikimrBridge::TClusterState::INACTIVE);

        const auto& details = config.GetClusterStateDetails();
        UNIT_ASSERT_C(!details.HasPendingPileOperation(), details.DebugString());
        for (const auto& pss : details.GetPileSyncState()) {
            UNIT_ASSERT_UNEQUAL(TBridgePileId::FromProto(&pss,
                &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId), pileId);
        }
        for (const auto& entry : details.GetUnsyncedHistory()) {
            for (const ui32 unsyncedPileId : entry.GetUnsyncedPiles()) {
                UNIT_ASSERT_UNEQUAL(unsyncedPileId, pileId.GetLocalDb());
            }
        }

        auto checkGroupState = [&](const NKikimrBridge::TGroupState& state) {
            UNIT_ASSERT_C(!NBridge::GetGroupPile(state, pileId), state.DebugString());
            UNIT_ASSERT_C(!state.PileSize() || !state.GetPile(state.PileSize() - 1).GetAbsent(), state.DebugString());
        };
        const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
        for (const auto& group : ss.GetGroups()) {
            if (group.HasBridgeGroupState()) {
                checkGroupState(group.GetBridgeGroupState());
            }
        }
        for (const auto& vdisk : ss.GetVDisks()) {
            if (inPile(vdisk.GetVDiskLocation().GetNodeID())) {
                UNIT_ASSERT_EQUAL_C(vdisk.GetEntityStatus(), NKikimrBlobStorage::EEntityStatus::DESTROY,
                    vdisk.DebugString());
            }
        }
        for (const auto *pb : {&config.GetStateStorageConfig(), &config.GetStateStorageBoardConfig(),
                &config.GetSchemeBoardConfig()}) {
            for (const auto& ringGroup : pb->GetRingGroups()) {
                UNIT_ASSERT_UNEQUAL(TBridgePileId::FromProto(&ringGroup,
                    &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId), pileId);
            }
        }

        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (TGroupID(vslot.GetGroupId()).ConfigurationType() == EGroupConfigurationType::Dynamic) {
                UNIT_ASSERT_C(!inPile(vslot.GetVSlotId().GetNodeId()), vslot.DebugString());
            }
        }
        for (const auto& group : baseConfig.GetGroup()) {
            if (TGroupID(group.GetGroupId()).ConfigurationType() == EGroupConfigurationType::Dynamic) {
                if (const auto info = GetGroupInfo(env, group.GetGroupId()); info.HasBridgeGroupState()) {
                    checkGroupState(info.GetBridgeGroupState());
                }
            }
        }
    }

    // nodes 1-9 of the converted cluster (see GetConvertedClusterLocation) get the name of their pile, then they are
    // restarted one by one with it; afterRestart is invoked after each restart
    inline void NameConvertedClusterNodes(TEnvironmentSetup& env, bool& named,
            const std::function<void()>& afterRestart = {}) {
        const auto res = ReplaceMainConfig(env, [&](YAML::Node& config) {
            YAML::Node pile;
            pile["name"] = "pile_1";
            config["bridge_config"]["piles"].push_back(pile);
            if (env.Runtime->GetAppDataBridgeConfig().GetEnableNonUniformStorage()) {
                config["bridge_config"]["enable_non_uniform_storage"] = true; // activation can't be dropped
            }
            for (YAML::Node host : config["hosts"]) {
                host["location"]["bridge_pile_name"] = "pile_1";
            }
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        named = true;
        for (ui32 nodeId = 1; nodeId <= 9; ++nodeId) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            if (afterRestart) {
                afterRestart();
            }
        }
    }

    // read data written by the checker from the parts of groups in the pile, not through the bridge proxy
    inline void CheckPileData(TEnvironmentSetup& env, const TDataChecker& checker, ui32 pileIndex, ui32 nodeId) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        for (const auto& [groupId, id, data] : checker.GetWritten()) {
            const auto info = GetGroupInfo(env, groupId);
            const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(pileIndex));
            UNIT_ASSERT(pile);
            const ui32 pileGroupId = pile->GetGroupId();
            const ui32 generation = GetGroupInfo(env, pileGroupId).GetGroupGeneration();
            env.Runtime->WrapInActorContext(edge, [&] {
                auto *request = new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead);
                request->ForceGroupGeneration = generation;
                SendToBSProxy(edge, pileGroupId, request);
            });
            CheckGetResult(*env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false)->Get(), data);
        }
    }

    // nodes 1-9 make the cluster to be converted (as in MakeUnbridgedSettings), they get the name of their pile when
    // named is set; nodes 10-17 make the pile added later
    inline TNodeLocation GetConvertedClusterLocation(ui32 nodeId, bool named) {
        if (nodeId <= 9 && !named) {
            return GetUnbridgedLocation(nodeId);
        }
        NActorsInterconnect::TNodeLocation location;
        location.SetBridgePileName(nodeId <= 9 ? "pile_1" : "pile_2");
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

    // the cluster to be converted with the nodes of the pile added later (see GetConvertedClusterLocation)
    inline TEnvironmentSetup::TSettings MakeConvertedClusterSettings(const std::shared_ptr<bool>& named) {
        return {
            .NodeCount = 17,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = [named](ui32 nodeId) { return GetConvertedClusterLocation(nodeId, *named); },
            .SelfManagementConfig = true,
            .AutomaticBootstrap = true,
            .InitialConfiguredNodeCount = 9,
        };
    }

} // NBridgeTest
