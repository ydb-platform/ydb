#include "distconf_invoke.h"

#include <ydb/core/protos/bridge.pb.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    void TInvokeRequestHandlerActor::NeedBridgeMode() {
        if (!Self->Cfg->BridgeConfig) {
            throw TExError() << "Bridge mode is not enabled";
        }
    }

    void TInvokeRequestHandlerActor::SwitchBridgeClusterState(const TQuery::TSwitchBridgeClusterState& cmd) {
        NeedBridgeMode();

        RunCommonChecks(/*requireScepter=*/ false);

        const auto& newClusterState = cmd.GetNewClusterState();

        // check new config alone
        const ui32 numPiles = Self->Cfg->BridgeConfig->PilesSize();
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
            for (ui32 i = 0; i < numPiles; ++i) {
                const auto currentState = current.GetPerPileState(i);
                const auto newState = newClusterState.GetPerPileState(i);
                if (currentState != newState) {
                    ++numDifferent;
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

                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                        case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                            Y_ABORT();
                    }
                }
            }
            if (numDifferent > 1) {
                throw TExError() << "Too many state changes in new configuration";
            } else if (current.GetGeneration() + 1 != newClusterState.GetGeneration()) {
                throw TExError() << "New cluster state generation# " << newClusterState.GetGeneration()
                    << " expected# " << current.GetGeneration() + 1;
            }
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto *clusterState = config.MutableClusterState();
        size_t changedPileIndex = Max<size_t>();
        for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
            if (clusterState->GetPerPileState(i) != newClusterState.GetPerPileState(i)) {
                Y_ABORT_UNLESS(changedPileIndex == Max<size_t>());
                changedPileIndex = i;
            }
        }
        clusterState->CopyFrom(newClusterState);

        if (changedPileIndex != Max<size_t>()) {
            auto *details = config.MutableClusterStateDetails();

            switch (clusterState->GetPerPileState(changedPileIndex)) {
                case NKikimrBridge::TClusterState::DISCONNECTED:
                case NKikimrBridge::TClusterState::SUSPENDED:
                    // this pile is not disconnected, there is no reason to synchronize it anymore
                    for (size_t i = 0; i < details->PileSyncStateSize(); ++i) {
                        const auto& item = details->GetPileSyncState(i);
                        const auto bridgePileId = TBridgePileId::FromProto(&item, &std::decay_t<decltype(item)>::GetBridgePileId);
                        if (bridgePileId.GetPileIndex() != changedPileIndex) {
                            break;
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

                        // spin generations for all static groups and update this pile's state to BLOCKS
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
                            Y_ABORT_UNLESS(changedPileIndex < state->PileSize());
                            auto *pile = state->MutablePile(changedPileIndex);
                            pile->ClearStage();
                            pile->SetBecameUnsyncedGeneration(clusterState->GetGeneration() + 1);
                            group->SetGroupGeneration(group->GetGroupGeneration() + 1);
                            for (auto& pile : *state->MutablePile()) {
                                const auto refGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                                auto *refGroup = groupMap[refGroupId];
                                Y_ABORT_UNLESS(refGroup);
                                Y_ABORT_UNLESS(refGroup->GetGroupGeneration() == pile.GetGroupGeneration());
                                refGroup->SetGroupGeneration(refGroup->GetGroupGeneration() + 1);
                                pile.SetGroupGeneration(refGroup->GetGroupGeneration());
                                changedGroupIds.insert(refGroupId);
                            }
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
                    break;

                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                    break;

                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                    Y_ABORT("invalid transition");

                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT();
            }
        }

        StartProposition(&config, /*mindPrev=*/ false);
    }

    void TInvokeRequestHandlerActor::NotifyBridgeSyncFinished(const TQuery::TNotifyBridgeSyncFinished& cmd) {
        RunCommonChecks();

        const auto bridgePileId = TBridgePileId::FromProto(&cmd, &TQuery::TNotifyBridgeSyncFinished::GetBridgePileId);

        if (!Self->Cfg->BridgeConfig) {
            throw TExError() << "Bridge mode is not enabled";
        } else if (Self->Cfg->BridgeConfig->PilesSize() <= bridgePileId.GetPileIndex()) {
            throw TExError() << "BridgePileId out of bounds";
        } else if (Self->StorageConfig->GetClusterState().GetGeneration() != cmd.GetGeneration()) {
            throw TExError() << "Generation mismatch";
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

        auto *details = config.MutableClusterStateDetails();

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
        bridgeProxyGroup->SetGroupGeneration(bridgeProxyGroup->GetGroupGeneration() + 1);
        for (auto& pile : *state->MutablePile()) {
            const auto it = groupMap.find(TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId));
            Y_ABORT_UNLESS(it != groupMap.end());
            pile.SetGroupGeneration(it->second->GetGroupGeneration());
        }

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

        if (!Self->Cfg->BridgeConfig) {
            throw TExError() << "Bridge mode is not enabled";
        } else if (Self->Cfg->BridgeConfig->PilesSize() <= pileId.GetPileIndex()) {
            throw TExError() << "BridgePileId out of bounds";
        } else if (Self->StorageConfig->GetClusterState().GetGeneration() != cmd.GetGeneration()) {
            throw TExError() << "Generation mismatch";
        }

        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        auto* clusterState = config.MutableClusterState();

        const bool wasSuspended = clusterState->GetPerPileState(pileId.GetPileIndex()) == NKikimrBridge::TClusterState::SUSPENDED;

        if (!wasSuspended) {
            return Finish(TResult::OK, std::nullopt);
        }

        clusterState->SetPerPileState(pileId.GetPileIndex(), NKikimrBridge::TClusterState::DISCONNECTED);
        StartProposition(&config, /*mindPrev=*/ false);
    }

} // NKikimr::NStorage
