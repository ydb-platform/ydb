#include "distconf_invoke.h"

#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::NeedBridgeMode() {
        if (!Self->Cfg->BridgeConfig) {
            throw TExError() << "Bridge mode is not enabled";
        } else if (!Self->StorageConfig || !Self->StorageConfig->HasClusterState()) {
            throw TExError() << "Cluster is not converted to bridged one yet";
        }
    }

    void TInvokeRequestHandlerActor::CheckPileNotification(TBridgePileId bridgePileId, ui64 generation) {
        NeedBridgeMode();
        if (!bridgePileId || Self->BridgeInfo->Piles.size() <= bridgePileId.GetPileIndex()) {
            throw TExError() << "BridgePileId out of bounds";
        } else if (Self->StorageConfig->GetClusterState().GetGeneration() != generation) {
            throw TExError() << "Generation mismatch";
        }
    }

    void TInvokeRequestHandlerActor::SwitchBridgeClusterState(const TQuery::TSwitchBridgeClusterState& cmd) {
        NeedBridgeMode();

        RunCommonChecks(/*requireScepter=*/ false);

        const auto& newClusterState = cmd.GetNewClusterState();

        // check new config alone
        Y_ABORT_UNLESS(Self->BridgeInfo);
        const ui32 numPiles = Self->BridgeInfo->Piles.size();
        const auto primaryPileId = TBridgePileId::FromProto(&newClusterState, &NKikimrBridge::TClusterState::GetPrimaryPile);
        const auto promotedPileId = TBridgePileId::FromProto(&newClusterState, &NKikimrBridge::TClusterState::GetPromotedPile);
        if (newClusterState.PerPileStateSize() != numPiles) {
            throw TExError() << "Incorrect number of per-pile states in new config";
        } else if (primaryPileId.GetPileIndex() >= numPiles) {
            throw TExError() << "Incorrect primary pile";
        } else if (promotedPileId.GetPileIndex() >= numPiles) {
            throw TExError() << "Incorrect promoted pile";
        } else if (newClusterState.GetPerPileState(primaryPileId.GetPileIndex()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            throw TExError() << "Incorrect primary pile state";
        } else if (newClusterState.GetPerPileState(promotedPileId.GetPileIndex()) != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            throw TExError() << "Incorrect promoted pile state";
        }

        if (Self->StorageConfig->HasClusterState()) {
            const NKikimrBridge::TClusterState& current = Self->StorageConfig->GetClusterState();
            Y_ABORT_UNLESS(current.PerPileStateSize() == numPiles);
            ui32 numDifferent = 0;
            bool onlyDisconnects = true;
            for (ui32 i = 0; i < numPiles; ++i) {
                const auto currentState = current.GetPerPileState(i);
                const auto newState = newClusterState.GetPerPileState(i);
                if (currentState != newState) {
                    ++numDifferent;
                    onlyDisconnects &= newState == NKikimrBridge::TClusterState::DISCONNECTED;
                    if (currentState == NKikimrBridge::TClusterState::INACTIVE) {
                        throw TExError() << "Pile '" << Self->BridgeInfo->Piles[i].Name
                            << "' is not added to the cluster";
                    } else if (NBridge::GetPendingPileOperationPileId(Self->StorageConfig->GetClusterStateDetails()) ==
                            TBridgePileId::FromPileIndex(i)) {
                        throw TExError() << "Pile '" << Self->BridgeInfo->Piles[i].Name
                            << "' is being added to the cluster, its state can't be changed until BSC finishes";
                    }
                    switch (newState) {
                        case NKikimrBridge::TClusterState::DISCONNECTED:
                            // valid transition from any state
                            break;

                        case NKikimrBridge::TClusterState::SYNCHRONIZED:
                            throw TExError() << "Can't switch to SYNCHRONIZED directly";

                        case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                            if (currentState == NKikimrBridge::TClusterState::SYNCHRONIZED) {
                                throw TExError() << "Invalid transition from SYNCHRONIZED to NOT_SYNCHRONIZED_1";
                            }
                            break;

                        case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                            throw TExError() << "Can't switch to NOT_SYNCHRONIZED_2 directly";

                        case NKikimrBridge::TClusterState::SUSPENDED:
                            // explicit SUSPENDED request is allowed
                            break;

                        case NKikimrBridge::TClusterState::INACTIVE:
                            throw TExError() << "Can't switch to INACTIVE, pile has to be removed from the cluster";

                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                            Y_ABORT();
                    }
                }
            }
            // Failed piles must be excluded together: excluding only one still requires
            // a configuration quorum in the other failed piles. Other transitions stay serial.
            if (numDifferent > 1 && !onlyDisconnects) {
                throw TExError() << "Too many state changes in new configuration";
            } else if (current.GetGeneration() + 1 != newClusterState.GetGeneration()) {
                throw TExError() << "New cluster state generation# " << newClusterState.GetGeneration()
                    << " expected# " << current.GetGeneration() + 1;
            }
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto *clusterState = config.MutableClusterState();
        std::vector<size_t> changedPileIndices;
        for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
            if (clusterState->GetPerPileState(i) != newClusterState.GetPerPileState(i)) {
                changedPileIndices.push_back(i);
            }
        }
        clusterState->CopyFrom(newClusterState);

        std::vector<size_t> disconnectedPileIndices;
        for (size_t changedPileIndex : changedPileIndices) {
            auto *details = config.MutableClusterStateDetails();

            switch (clusterState->GetPerPileState(changedPileIndex)) {
                case NKikimrBridge::TClusterState::DISCONNECTED:
                case NKikimrBridge::TClusterState::SUSPENDED:
                    // This pile no longer participates in replication.
                    for (size_t i = 0; i < details->PileSyncStateSize(); ++i) {
                        const auto& item = details->GetPileSyncState(i);
                        const auto bridgePileId = TBridgePileId::FromProto(&item, &std::decay_t<decltype(item)>::GetBridgePileId);
                        if (bridgePileId.GetPileIndex() != changedPileIndex) {
                            continue;
                        }
                        details->MutablePileSyncState()->DeleteSubrange(i, 1);
                        break;
                    }
                    {
                        // mark pile unsynced
                        auto *state = details->AddPileSyncState();
                        TBridgePileId::FromPileIndex(changedPileIndex).CopyToProto(state,
                            &std::decay_t<decltype(*state)>::SetBridgePileId);
                        state->SetBecameUnsyncedGeneration(clusterState->GetGeneration());
                        state->SetUnsyncedBSC(true);

                        disconnectedPileIndices.push_back(changedPileIndex);
                    }
                    break;

                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                    break;

                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                case NKikimrBridge::TClusterState::INACTIVE:
                    Y_ABORT("invalid transition");

                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
            }
        }

        if (!disconnectedPileIndices.empty()) {
            // Bump each static group once per configuration, even when several piles fail.
            auto *ss = config.MutableBlobStorageConfig()->MutableServiceSet();
            THashMap<TGroupId, NKikimrBlobStorage::TGroupInfo*> groupMap;
            for (auto& group : *ss->MutableGroups()) {
                groupMap.emplace(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID), &group);
            }
            THashSet<TGroupId> changedGroupIds;
            for (const auto& [groupId, group] : groupMap) {
                if (!group->HasBridgeGroupState()) {
                    continue;
                }
                auto *state = group->MutableBridgeGroupState();
                for (size_t pileIndex : disconnectedPileIndices) {
                    auto *pile = NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(pileIndex));
                    Y_ABORT_UNLESS(pile); // disconnected pile is not INACTIVE, so the group has its part there
                    pile->ClearStage();
                    pile->SetBecameUnsyncedGeneration(clusterState->GetGeneration() + 1);
                }
                NBridge::ForEachGroupPile(state, [&](TBridgePileId, NKikimrBridge::TGroupState::TPile& pile) {
                    const auto refGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                    auto *refGroup = groupMap[refGroupId];
                    Y_ABORT_UNLESS(refGroup);
                    Y_ABORT_UNLESS(refGroup->GetGroupGeneration() == pile.GetGroupGeneration());
                    refGroup->SetGroupGeneration(refGroup->GetGroupGeneration() + 1);
                    pile.SetGroupGeneration(refGroup->GetGroupGeneration());
                    changedGroupIds.insert(refGroupId);
                });
                BumpBridgeProxyGeneration(group, [&](TGroupId id) { return changedGroupIds.contains(id); });
            }
            for (auto& vdisk : *ss->MutableVDisks()) {
                auto vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (!changedGroupIds.contains(vdiskId.GroupID)) {
                    continue;
                }
                auto *group = groupMap[vdiskId.GroupID];
                Y_ABORT_UNLESS(group);
                vdiskId.GroupGeneration = group->GetGroupGeneration();
                VDiskIDFromVDiskID(vdiskId, vdisk.MutableVDiskID());
            }
        }

        StartProposition(&config, /*mindPrev=*/ false);
    }

    void TInvokeRequestHandlerActor::NotifyBridgeSyncFinished(const TQuery::TNotifyBridgeSyncFinished& cmd) {
        RunCommonChecks();

        const auto bridgePileId = TBridgePileId::FromProto(&cmd, &TQuery::TNotifyBridgeSyncFinished::GetBridgePileId);
        CheckPileNotification(bridgePileId, cmd.GetGeneration());

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        auto *details = config.MutableClusterStateDetails();
        if (NBridge::GetPendingPileOperationPileId(*details) == bridgePileId) {
            throw TExError() << "Pile has pending operation"; // BSC has not created all the parts of its groups yet
        }

        for (auto& state : *details->MutablePileSyncState()) {
            if (TBridgePileId::FromProto(&state, &std::decay_t<decltype(state)>::GetBridgePileId) == bridgePileId) {
                if (cmd.HasBSC()) {
                    if (!cmd.GetBSC()) {
                        throw TExError() << "Incorrect request";
                    }
                    state.SetUnsyncedBSC(false);
                }
                return StartProposition(&config);
            }
        }

        throw TExError() << "Unsynced pile not found";
    }

    void TInvokeRequestHandlerActor::UpdateBridgeGroupInfo(const TQuery::TUpdateBridgeGroupInfo& cmd) {
        RunCommonChecks();
        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        // find group we are going to update
        auto *bsConfig = config.MutableBlobStorageConfig();
        auto *ss = bsConfig->MutableServiceSet();
        auto *groups = ss->MutableGroups();
        NKikimrBlobStorage::TGroupInfo *bridgeProxyGroup = nullptr;
        THashMap<TGroupId, NKikimrBlobStorage::TGroupInfo*> groupMap;
        for (auto& group : *groups) {
            if (group.GetGroupID() == cmd.GetGroupId()) {
                if (group.GetGroupGeneration() != cmd.GetGroupGeneration()) {
                    throw TExError() << "bridge proxy group generation mismatch";
                } else if (!group.HasBridgeGroupState()) {
                    throw TExError() << "group is not bridge proxy group";
                }
                bridgeProxyGroup = &group;
            }
            groupMap.emplace(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID), &group);
        }
        if (!bridgeProxyGroup) {
            throw TExError() << "bridge proxy group not found";
        }

        // validate state update
        auto *state = bridgeProxyGroup->MutableBridgeGroupState();
        const auto& newState = cmd.GetBridgeGroupInfo().GetBridgeGroupState();
        if (state->PileSize() != newState.PileSize()) {
            throw TExError() << "can't change number of piles in TGroupInfo.BridgeGroupState";
        }
        THashMap<TGroupId, ui32> referencedGroups;
        for (size_t i = 0; i < newState.PileSize(); ++i) {
            const auto& pile = newState.GetPile(i);
            if (pile.GetAbsent() != state->GetPile(i).GetAbsent()) {
                throw TExError() << "can't change set of piles in TGroupInfo.BridgeGroupState";
            } else if (pile.GetAbsent()) {
                continue; // the group has no part in this pile
            }
            const auto it = groupMap.find(TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId));
            if (it == groupMap.end()) {
                throw TExError() << "can't find referenced group";
            } else if (state->GetPile(i).GetGroupId() != pile.GetGroupId()) {
                throw TExError() << "can't change group id";
            } else if (it->second->GetGroupGeneration() != pile.GetGroupGeneration()) {
                throw TExError() << "referenced group generation mismatch";
            }
            it->second->SetGroupGeneration(it->second->GetGroupGeneration() + 1);
            referencedGroups.emplace(it->first, it->second->GetGroupGeneration());
        }

        // update state
        state->CopyFrom(newState);

        // update referenced group generations
        BumpBridgeProxyGeneration(bridgeProxyGroup, [&](TGroupId id) { return referencedGroups.contains(id); });
        NBridge::ForEachGroupPile(state, [&](TBridgePileId, NKikimrBridge::TGroupState::TPile& pile) {
            const auto it = groupMap.find(TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId));
            Y_ABORT_UNLESS(it != groupMap.end());
            pile.SetGroupGeneration(it->second->GetGroupGeneration());
        });

        // update vdisk generations
        for (auto& vdisk : *ss->MutableVDisks()) {
            auto vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
            if (const auto it = referencedGroups.find(vdiskId.GroupID); it != referencedGroups.end()) {
                vdiskId.GroupGeneration = it->second;
                VDiskIDFromVDiskID(vdiskId, vdisk.MutableVDiskID());
            }
        }

        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::NotifyBridgeSuspended(const TQuery::TNotifyBridgeSuspended& cmd) {
        RunCommonChecks();

        const auto pileId = TBridgePileId::FromProto(&cmd, &TQuery::TNotifyBridgeSuspended::GetPileId);
        CheckPileNotification(pileId, cmd.GetGeneration());

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto* clusterState = config.MutableClusterState();

        const bool wasSuspended = clusterState->GetPerPileState(pileId.GetPileIndex()) == NKikimrBridge::TClusterState::SUSPENDED;

        if (!wasSuspended) {
            return Finish(TResult::OK, std::nullopt);
        }

        clusterState->SetPerPileState(pileId.GetPileIndex(), NKikimrBridge::TClusterState::DISCONNECTED);
        StartProposition(&config, /*mindPrev=*/ false);
    }

    void TInvokeRequestHandlerActor::ConvertToBridge(const TQuery::TConvertToBridge& /*cmd*/) {
        RunCommonChecks();

        if (!Self->SelfManagementEnabled) {
            throw TExError() << "Self-management must be enabled to convert cluster to bridged one";
        } else if (!Self->Cfg->BridgeConfig) {
            throw TExError() << "Bridge mode is not enabled";
        } else if (Self->Cfg->BridgeConfig->PilesSize() != 1) {
            throw TExError() << "BridgeConfig must contain exactly one pile to convert cluster to bridged one";
        } else if (Self->StorageConfig->HasClusterState()) {
            throw TExError() << "Cluster is already bridged one";
        }

        // nodes running without BridgeConfig won't be able to work in bridged cluster
        THashSet<ui32> requiredNodeIds;
        for (const auto& node : Self->StorageConfig->GetAllNodes()) {
            requiredNodeIds.insert(node.GetNodeId());
        }
        RequireBridgeCapability(false, std::move(requiredNodeIds), {}, [this] {
            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
            if (auto error = ConvertStorageConfigToBridge(&config, *Self->Cfg->BridgeConfig)) {
                throw TExError() << *error;
            }
            StartProposition(&config);
        });
    }

    void TInvokeRequestHandlerActor::RequireBridgeCapability(bool pileListChange, THashSet<ui32> requiredNodeIds,
            THashSet<ui32> ignoredNodeIds, std::function<void()> callback) {
        // every required static node must positively report its capability, as the one being temporarily disconnected
        // may still run without it and return later; dynamic nodes can't be enumerated this way, the ones that are
        // missed here can't serve bridged groups until restarted; every node that is connected gets the new config,
        // so it must be capable even being ignored (that is, going to leave the cluster)
        TEvScatter task;
        task.MutableCollectBridgeCapability();
        IssueScatterTask(std::move(task), [this, pileListChange, requiredNodeIds = std::move(requiredNodeIds),
                ignoredNodeIds = std::move(ignoredNodeIds), callback = std::move(callback)](TEvGather *res) {
            if (!res->HasCollectBridgeCapability()) {
                throw TExError() << "Incorrect CollectBridgeCapability response";
            }
            const auto& response = res->GetCollectBridgeCapability();

            const auto& capableIds = pileListChange
                ? response.GetPileListChangeCapableNodeIds()
                : response.GetCapableNodeIds();
            const THashSet<ui32> capable(capableIds.begin(), capableIds.end());
            std::set<ui32> notCapable(response.GetNotCapableNodeIds().begin(), response.GetNotCapableNodeIds().end());
            for (const ui32 nodeId : response.GetCapableNodeIds()) {
                if (!capable.contains(nodeId)) {
                    notCapable.insert(nodeId); // node runs older version not supporting changes of the list of piles
                }
            }
            std::set<ui32> unknown;
            for (const ui32 nodeId : requiredNodeIds) {
                if (!capable.contains(nodeId) && !notCapable.contains(nodeId)) {
                    unknown.insert(nodeId); // node is not connected or runs older version that doesn't report anything
                }
            }
            for (const auto& [node, _] : Self->AllBoundNodes) {
                if (const ui32 nodeId = node.NodeId(); !capable.contains(nodeId) && !notCapable.contains(nodeId) &&
                        !ignoredNodeIds.contains(nodeId)) {
                    unknown.insert(nodeId);
                }
            }
            if (!notCapable.empty() || !unknown.empty()) {
                TStringBuilder err;
                if (!notCapable.empty()) {
                    err << (pileListChange
                        ? "Some connected nodes are running without BridgeConfig or with version not supporting changes"
                            " of the list of piles, they have to be upgraded or stopped first NodeIds#"
                        : "Some nodes are running without BridgeConfig NodeIds#");
                    for (const ui32 nodeId : notCapable) {
                        err << ' ' << nodeId;
                    }
                }
                if (!unknown.empty()) {
                    if (!notCapable.empty()) {
                        err << "; ";
                    }
                    err << "Some static nodes did not report their BridgeConfig, they must be running and connected"
                        " NodeIds#";
                    for (const ui32 nodeId : unknown) {
                        err << ' ' << nodeId;
                    }
                }
                throw TExError() << err;
            }

            callback();
        });
    }

    void TInvokeRequestHandlerActor::NotifyBscConversionFinished(const TQuery::TNotifyBscConversionFinished& /*cmd*/) {
        RunCommonChecks();

        if (!Self->StorageConfig->GetClusterStateDetails().GetBscConversionPending()) {
            return Finish(TResult::OK, std::nullopt); // already cleared
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        config.MutableClusterStateDetails()->ClearBscConversionPending();
        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::AddBridgePile(const TQuery::TAddBridgePile& cmd) {
        RunCommonChecks();
        NeedBridgeMode();

        if (!Self->SelfManagementEnabled) {
            throw TExError() << "Self-management must be enabled to add piles";
        }

        const auto& config = *Self->StorageConfig;
        const auto& clusterState = config.GetClusterState();
        const auto& details = config.GetClusterStateDetails();
        const auto registry = Self->GetConfigPileRegistry(config);
        Y_ABORT_UNLESS(registry); // the node runs with BridgeConfig

        const TBridgePileId pileId = NBridge::ResolvePileId(*registry, cmd.GetPileName());
        if (!pileId) {
            throw TExError() << "Pile '" << cmd.GetPileName() << "' is not listed in BridgeConfig";
        } else if (const auto state = NBridge::GetPileState(clusterState, pileId); state != NKikimrBridge::TClusterState::INACTIVE) {
            throw TExError() << "Pile '" << cmd.GetPileName() << "' is already added to the cluster, its state is "
                << NKikimrBridge::TClusterState::EPileState_Name(state);
        } else if (details.GetBscConversionPending()) {
            throw TExError() << "Piles can't be added until BSC finishes conversion of the cluster";
        } else if (const auto *pending = Self->BridgeInfo->FindPile(NBridge::GetPendingPileOperationPileId(details))) {
            throw TExError() << "Operation on pile '" << pending->Name << "' is in progress";
        }

        // every node of the cluster must know of pending pile operations and take pile ids from the registry; nodes of
        // INACTIVE piles (including the one being added) are not connected to the cluster
        THashSet<ui32> requiredNodeIds;
        THashSet<ui32> ignoredNodeIds;
        for (const auto& node : config.GetAllNodes()) {
            const auto bridgePileId = ResolveNodePileId(registry, TNodeLocation(node.GetLocation()));
            const bool inactive = !bridgePileId || NBridge::GetPileState(clusterState, *bridgePileId) ==
                NKikimrBridge::TClusterState::INACTIVE;
            (inactive ? ignoredNodeIds : requiredNodeIds).insert(node.GetNodeId());
        }
        RequireBridgeCapability(true, std::move(requiredNodeIds), std::move(ignoredNodeIds), [this, pileId] {
            // PDisks of the new pile are already known to BSC, static groups have to be allocated on the same ones
            AddedBridgePileId = pileId;
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryBaseConfig);
        });
    }

    void TInvokeRequestHandlerActor::AddBridgePileWithBaseConfig() {
        RunCommonChecks();

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        if (auto error = Self->AddBridgePileToConfig(&config, AddedBridgePileId, &BaseConfig.value())) {
            throw TExError() << "Can't add pile to the cluster: " << *error;
        }
        ProposedStorageConfig = std::move(config);

        // BSC checks whether it can add parts to its groups before anything is committed
        ControllerOp = EControllerOp::ADD_BRIDGE_PILE;
        ConnectToController();
    }

    void TInvokeRequestHandlerActor::NotifyBridgePileOperation(const TQuery::TNotifyBridgePileOperation& cmd) {
        RunCommonChecks();

        const auto& details = Self->StorageConfig->GetClusterStateDetails();
        const auto& op = details.GetPendingPileOperation();
        if (const auto pileId = NBridge::GetPendingPileOperationPileId(details); !pileId) {
            throw TExError() << "No pending pile operation";
        } else if (pileId != TBridgePileId::FromProto(&cmd, &TQuery::TNotifyBridgePileOperation::GetBridgePileId) ||
                op.GetKind() != cmd.GetKind()) {
            throw TExError() << "Pending pile operation is another one";
        } else if (cmd.HasErrorReason() && op.HasErrorReason() && op.GetErrorReason() == cmd.GetErrorReason()) {
            return Finish(TResult::OK, std::nullopt); // nothing changes
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto *mutableDetails = config.MutableClusterStateDetails();
        if (cmd.HasErrorReason()) {
            mutableDetails->MutablePendingPileOperation()->SetErrorReason(cmd.GetErrorReason());
        } else {
            mutableDetails->ClearPendingPileOperation();
        }
        StartProposition(&config);
    }

} // NKikimr::NStorage
