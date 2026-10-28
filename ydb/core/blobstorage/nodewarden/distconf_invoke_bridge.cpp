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
                    } else if (const auto& details = Self->StorageConfig->GetClusterStateDetails();
                            NBridge::GetPendingPileOperationPileId(details) == TBridgePileId::FromPileIndex(i)) {
                        const bool adding = details.GetPendingPileOperation().GetKind() ==
                            NKikimrBridge::TClusterStateDetails::TPendingPileOperation::ADD;
                        throw TExError() << "Pile '" << Self->BridgeInfo->Piles[i].Name << "' is being "
                            << (adding ? "added to" : "removed from")
                            << " the cluster, its state can't be changed until BSC finishes";
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
        auto *details = config.MutableClusterStateDetails();
        std::vector<std::pair<size_t, NKikimrBridge::TClusterState::EPileState>> changedPiles;
        for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
            if (clusterState->GetPerPileState(i) != newClusterState.GetPerPileState(i)) {
                changedPiles.emplace_back(i, clusterState->GetPerPileState(i));
            }
        }
        clusterState->CopyFrom(newClusterState);

        auto markPileUnsynced = [&](size_t pileIndex) {
            for (size_t i = 0; i < details->PileSyncStateSize(); ++i) {
                const auto& item = details->GetPileSyncState(i);
                const auto bridgePileId = TBridgePileId::FromProto(&item, &std::decay_t<decltype(item)>::GetBridgePileId);
                if (bridgePileId.GetPileIndex() != pileIndex) {
                    continue;
                }
                details->MutablePileSyncState()->DeleteSubrange(i, 1);
                break;
            }
            auto *state = details->AddPileSyncState();
            TBridgePileId::FromPileIndex(pileIndex).CopyToProto(state,
                &std::decay_t<decltype(*state)>::SetBridgePileId);
            state->SetBecameUnsyncedGeneration(clusterState->GetGeneration());
            state->SetUnsyncedBSC(true);
        };

        std::vector<size_t> disconnectedPileIndices;
        for (const auto& [changedPileIndex, prevState] : changedPiles) {
            switch (clusterState->GetPerPileState(changedPileIndex)) {
                case NKikimrBridge::TClusterState::DISCONNECTED:
                case NKikimrBridge::TClusterState::SUSPENDED:
                    // This pile no longer participates in replication.
                    markPileUnsynced(changedPileIndex);
                    disconnectedPileIndices.push_back(changedPileIndex);
                    break;

                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                    if (prevState == NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2) {
                        // The pile is being synced and its groups have got their parts resynced already, which BSC has
                        // reported (or is about to). The re-run of rejoin makes writers skip the pile without bumping
                        // the generations of its parts, so the parts would keep the SYNCED stage of their previous
                        // sync while missing the writes made meanwhile; the pile has to go through the whole sync
                        // again, like when it is cut off.
                        markPileUnsynced(changedPileIndex);
                    }
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
            std::vector<TBridgePileId> pileIds;
            for (size_t pileIndex : disconnectedPileIndices) {
                pileIds.push_back(TBridgePileId::FromPileIndex(pileIndex));
            }
            ResetStaticGroupPileParts(&config, pileIds, clusterState->GetGeneration());
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
        UpdateStaticVDiskGenerations(ss, referencedGroups);

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

    void TInvokeRequestHandlerActor::ConvertToBridge(const TQuery::TConvertToBridge& cmd) {
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

        // nodes running without BridgeConfig won't be able to work in bridged cluster, as well as the ones of versions
        // not supporting the features the converted cluster requires
        THashSet<ui32> requiredNodeIds;
        for (const auto& node : Self->StorageConfig->GetAllNodes()) {
            requiredNodeIds.insert(node.GetNodeId());
        }
        const bool skipStoragePoolConversion = cmd.GetSkipStoragePoolConversion();
        RequireBridgeCapability(BridgeFeatureLevel, std::move(requiredNodeIds), {}, [this, skipStoragePoolConversion] {
            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
            if (auto error = ConvertStorageConfigToBridge(&config, *Self->Cfg->BridgeConfig,
                    skipStoragePoolConversion)) {
                throw TExError() << *error;
            }
            StartProposition(&config);
        });
    }

    void TInvokeRequestHandlerActor::RequireBridgeCapability(ui32 minFeatureLevel, THashSet<ui32> requiredNodeIds,
            THashSet<ui32> ignoredNodeIds, std::function<void()> callback) {
        // every required static node must positively report its capability, as the one being temporarily disconnected
        // may still run without it and return later; dynamic nodes can't be enumerated this way, the ones that are
        // missed here can't serve bridged groups until restarted; every node that is connected gets the new config,
        // so it must be capable even being ignored (that is, going to leave the cluster)
        TEvScatter task;
        task.MutableCollectBridgeCapability();
        IssueScatterTask(std::move(task), [this, minFeatureLevel, requiredNodeIds = std::move(requiredNodeIds),
                ignoredNodeIds = std::move(ignoredNodeIds), callback = std::move(callback)](TEvGather *res) {
            if (!res->HasCollectBridgeCapability()) {
                throw TExError() << "Incorrect CollectBridgeCapability response";
            }
            const auto& response = res->GetCollectBridgeCapability();

            // older versions don't report feature levels, but the ones supporting changes of the list of piles say so
            THashMap<ui32, ui32> levels;
            for (const ui32 nodeId : response.GetPileListChangeCapableNodeIds()) {
                levels[nodeId] = BridgeFeatureLevelPileList;
            }
            for (const auto& item : response.GetFeatureLevels()) {
                levels[item.GetNodeId()] = item.GetLevel();
            }
            THashSet<ui32> capable;
            std::set<ui32> notCapable(response.GetNotCapableNodeIds().begin(), response.GetNotCapableNodeIds().end());
            for (const ui32 nodeId : response.GetCapableNodeIds()) {
                const auto it = levels.find(nodeId);
                if (!minFeatureLevel || (it != levels.end() && it->second >= minFeatureLevel)) {
                    capable.insert(nodeId);
                } else {
                    notCapable.insert(nodeId); // node runs older version not supporting the required bridge features
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
                    err << (minFeatureLevel
                        ? "Some connected nodes are running without BridgeConfig or with version not supporting the"
                            " bridge features the operation requires, they have to be upgraded or stopped first NodeIds#"
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

    TBridgePileId TInvokeRequestHandlerActor::CheckPileOperation(const TString& pileName,
            NKikimrBridge::TClusterState::EPileState expectedState, bool adding) {
        RunCommonChecks();
        NeedBridgeMode();

        const char *verb = adding ? "added to" : "removed from";
        if (!Self->SelfManagementEnabled) {
            throw TExError() << "Self-management must be enabled for piles to be " << verb << " the cluster";
        }

        const auto& config = *Self->StorageConfig;
        const auto& details = config.GetClusterStateDetails();
        const auto registry = Self->GetConfigPileRegistry(config);
        Y_ABORT_UNLESS(registry); // the node runs with BridgeConfig

        const TBridgePileId pileId = NBridge::ResolvePileId(*registry, pileName);
        if (!pileId) {
            throw TExError() << "Pile '" << pileName << "' is not listed in BridgeConfig";
        } else if (const auto *pending = Self->BridgeInfo->FindPile(NBridge::GetPendingPileOperationPileId(details));
                pending && !(!adding && pending->BridgePileId == pileId && details.GetPendingPileOperation().GetKind() ==
                    NKikimrBridge::TClusterStateDetails::TPendingPileOperation::ADD)) {
            // the addition stuck (e.g. as resources have gone) is cancelled by removing the pile, which replaces it
            throw TExError() << "Operation on pile '" << pending->Name << "' is in progress";
        } else if (const auto state = NBridge::GetPileState(config.GetClusterState(), pileId); state != expectedState) {
            throw TExError() << "Pile '" << pileName << "' has to be " << NKikimrBridge::TClusterState::EPileState_Name(
                expectedState) << " to be " << verb << " the cluster, its state is "
                << NKikimrBridge::TClusterState::EPileState_Name(state);
        } else if (details.GetBscConversionPending()) {
            throw TExError() << "Piles can't be " << verb << " the cluster until BSC finishes conversion of the cluster";
        }
        return pileId;
    }

    void TInvokeRequestHandlerActor::RequirePileOperationCapability(TBridgePileId leavingPileId,
            std::function<void()> callback) {
        // every node connected to the cluster has to support pile operations; nodes of INACTIVE piles (including the one
        // being added), of DISCONNECTED ones (including the one being removed) can't connect, and the ones of lower
        // feature level are refused when their pile is back (connections are checked against the required level)
        const auto& config = *Self->StorageConfig;
        const auto registry = Self->GetConfigPileRegistry(config);
        THashSet<ui32> requiredNodeIds;
        THashSet<ui32> ignoredNodeIds;
        for (const auto& node : config.GetAllNodes()) {
            const auto bridgePileId = ResolveNodePileId(registry, TNodeLocation(node.GetLocation()));
            const bool outside = !bridgePileId || *bridgePileId == leavingPileId || !NBridge::PileStateTraits(
                NBridge::GetPileState(config.GetClusterState(), *bridgePileId)).AllowsConnection;
            (outside ? ignoredNodeIds : requiredNodeIds).insert(node.GetNodeId());
        }
        RequireBridgeCapability(BridgeFeatureLevelPileOps, std::move(requiredNodeIds), std::move(ignoredNodeIds),
            std::move(callback));
    }

    void TInvokeRequestHandlerActor::AddBridgePile(const TQuery::TAddBridgePile& cmd) {
        const TBridgePileId pileId = CheckPileOperation(cmd.GetPileName(), NKikimrBridge::TClusterState::INACTIVE, true);
        RequirePileOperationCapability(TBridgePileId(), [this, pileId] {
            // PDisks of the new pile are already known to BSC, static groups have to be allocated on the same ones
            BridgePileOpPileId = pileId;
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryBaseConfig);
        });
    }

    void TInvokeRequestHandlerActor::AddBridgePileWithBaseConfig() {
        RunCommonChecks();

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        if (auto error = Self->AddBridgePileToConfig(&config, BridgePileOpPileId, &BaseConfig.value())) {
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
                op.GetKind() != cmd.GetKind() || op.GetOperationId() != cmd.GetOperationId()) {
            // the notification may come late, when the operation it refers to has already been finished
            throw TExError() << "Pending pile operation is another one";
        } else if (cmd.HasErrorReason() && op.HasErrorReason() && op.GetErrorReason() == cmd.GetErrorReason()) {
            return Finish(TResult::OK, std::nullopt); // nothing changes
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto *mutableDetails = config.MutableClusterStateDetails();
        if (cmd.HasErrorReason()) {
            mutableDetails->MutablePendingPileOperation()->SetErrorReason(cmd.GetErrorReason());
        } else if (op.GetKind() == NKikimrBridge::TClusterStateDetails::TPendingPileOperation::REMOVE) {
            // BSC has deleted parts of its groups in the pile, static ones go away now
            if (auto error = RemoveBridgePileFromConfig(&config, NBridge::GetPendingPileOperationPileId(details))) {
                throw TExError() << "Can't remove pile from the cluster: " << *error;
            }
        } else {
            mutableDetails->ClearPendingPileOperation();
        }
        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::RemoveBridgePile(const TQuery::TRemoveBridgePile& cmd) {
        const TBridgePileId pileId = CheckPileOperation(cmd.GetPileName(), NKikimrBridge::TClusterState::DISCONNECTED,
            false);

        // nodes without pile name belong to the implicit pile, they can't be resolved without it
        const auto& config = *Self->StorageConfig;
        const auto registry = Self->GetConfigPileRegistry(config);
        for (const auto& node : config.GetAllNodes()) {
            if (const TNodeLocation location(node.GetLocation()); !location.GetBridgePileName() &&
                    ResolveNodePileId(registry, location) == pileId) {
                throw TExError() << "Pile '" << cmd.GetPileName() << "' is the implicit one and NodeId# "
                    << node.GetNodeId() << " has no pile name, nodes have to get the name of their pile first";
            }
        }

        // data of static VDisks in the pile is lost with it, so they have to belong to parts of bridged groups
        THashMap<TGroupId, TBridgePileId> groupPiles;
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            groupPiles.emplace(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID),
                TBridgePileId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId));
        }
        for (const auto& vdisk : config.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
            const auto groupId = VDiskIDFromVDiskID(vdisk.GetVDiskID()).GroupID;
            if (vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                continue;
            } else if (const auto it = groupPiles.find(groupId); it != groupPiles.end() && it->second == pileId) {
                continue;
            } else if (const auto *pile = Self->BridgeInfo->GetPileForNode(vdisk.GetVDiskLocation().GetNodeID());
                    pile && pile->BridgePileId == pileId) {
                throw TExError() << "Static GroupId# " << groupId << " has VDisks in pile '" << cmd.GetPileName()
                    << "', but it is not a part of bridged group";
            }
        }

        RequirePileOperationCapability(pileId, [this, pileId] {
            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
            auto *details = config.MutableClusterStateDetails();
            auto *op = details->MutablePendingPileOperation();
            op->Clear(); // the pending addition of this pile, if any, is replaced along with its error
            pileId.CopyToProto(op, &NKikimrBridge::TClusterStateDetails::TPendingPileOperation::SetBridgePileId);
            op->SetKind(NKikimrBridge::TClusterStateDetails::TPendingPileOperation::REMOVE);
            op->SetOperationId(config.GetGeneration() + 1); // the generation of the config being proposed
            // nodes of older versions can't work with INACTIVE piles and removed static groups
            auto *registry = details->MutablePileRegistry();
            registry->SetPileListChanged(true);
            registry->SetRequiredFeatureLevel(Max<ui32>(registry->GetRequiredFeatureLevel(), BridgeFeatureLevelPileOps));
            ProposedStorageConfig = std::move(config);

            // BSC checks whether it can delete parts of its groups before anything is committed
            BridgePileOpPileId = pileId;
            ControllerOp = EControllerOp::REMOVE_BRIDGE_PILE;
            ConnectToController();
        });
    }

} // NKikimr::NStorage
