#include "distconf.h"
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NStorage {

    bool TDistributedConfigKeeper::UpdateBridgeConfig(NKikimrBlobStorage::TStorageConfig *config) {
        bool changes = false;

        if (config->HasClusterState()) {
            auto *clusterState = config->MutableClusterState();
            if (clusterState->GetPrimaryPile() != clusterState->GetPromotedPile()) {
                const auto promotedPileId = TBridgePileId::FromProto(clusterState, &NKikimrBridge::TClusterState::GetPromotedPile);
                if (promotedPileId.GetPileIndex() < clusterState->PerPileStateSize() &&
                        clusterState->GetPerPileState(promotedPileId.GetPileIndex()) == NKikimrBridge::TClusterState::SYNCHRONIZED) {
                    // TODO(alexvru): some external conditions? wait for external components?
                    promotedPileId.CopyToProto(clusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
                    changes = true;
                }
            }
        }

        if (config->HasClusterStateDetails()) {
            auto *details = config->MutableClusterStateDetails();

            Y_DEBUG_ABORT_UNLESS(config->HasClusterState());
            auto *clusterState = config->MutableClusterState();

            // switch unsynced piles (NOT_SYNCHRONIZED_1) to NOT_SYNCHRONIZED_2 when connected
            for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                if (clusterState->GetPerPileState(i) == NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1 &&
                        ConnectedUnsyncedPiles.contains(TBridgePileId::FromPileIndex(i))) {
                    clusterState->SetPerPileState(i, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
                    changes = true;

                    auto *state = details->AddPileSyncState();
                    TBridgePileId::FromPileIndex(i).CopyToProto(state, &std::decay_t<decltype(*state)>::SetBridgePileId);
                    state->SetBecameUnsyncedGeneration(clusterState->GetGeneration() + 1);
                    state->SetUnsyncedBSC(true);

                    // spin generations for all static groups and update this pile's state to BLOCKS
                    auto *ss = config->MutableBlobStorageConfig()->MutableServiceSet();
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
                        Y_ABORT_UNLESS(i < state->PileSize());
                        auto *pile = state->MutablePile(i);
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
            }
        }

        return changes;
    }

    void TDistributedConfigKeeper::GenerateBridgeInitialState(const TNodeWardenConfig& cfg,
            NKikimrBlobStorage::TStorageConfig *config) {
        if (!cfg.BridgeConfig) {
            return; // no bridge mode enabled at all
        } else if (config->HasClusterState()) {
            return; // some cluster state has been already defined
        }

        auto *state = config->MutableClusterState();
        auto *piles = state->MutablePerPileState();
        for (size_t i = 0; i < cfg.BridgeConfig->PilesSize(); ++i) {
            piles->Add(NKikimrBridge::TClusterState::SYNCHRONIZED);
        }
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPromotedPile);

        config->MutableClusterStateDetails();
    }

    std::optional<TString> TDistributedConfigKeeper::TransformConfigBeforeCommit(NKikimrBlobStorage::TStorageConfig *config) {
        if (auto error = CheckSynchronizedPiles(config)) { // can change cluster state
            return error;
        } else if (auto error = UpdateClusterStateGeneration(config)) { // transforms cluster state change into unsynced history
            return error;
        } else if (auto error = TrimHistoryEntries(config)) { // trims unsynced history entries
            return error;
        } else if (auto error = ExtractStateStorageConfig(config)) { // extract final state storage config
            return error;
        }
        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::CheckSynchronizedPiles(NKikimrBlobStorage::TStorageConfig *config) {
        if (config->HasClusterStateDetails()) {
            auto *pileSyncState = config->MutableClusterStateDetails()->MutablePileSyncState();
            for (int stateIndex = 0; stateIndex < pileSyncState->size(); ++stateIndex) {
                auto *state = pileSyncState->Mutable(stateIndex);
                const auto bridgePileId = TBridgePileId::FromProto(state, &std::decay_t<decltype(*state)>::GetBridgePileId);

                auto checkIfStaticGroupsSynced = [&] {
                    for (const auto& groupInfo : config->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                        if (!groupInfo.HasBridgeGroupState()) {
                            continue;
                        }
                        const auto& groupState = groupInfo.GetBridgeGroupState();
                        if (groupState.PileSize() <= bridgePileId.GetPileIndex()) {
                            Y_DEBUG_ABORT();
                            return false;
                        }
                        const auto& pile = groupState.GetPile(bridgePileId.GetPileIndex());
                        if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                            return false;
                        }
                    }
                    return true;
                };

                if (!state->GetUnsyncedBSC() && checkIfStaticGroupsSynced()) {
                    // fully synced, can switch to SYNCHRONIZED
                    pileSyncState->DeleteSubrange(stateIndex--, 1);
                    config->MutableClusterState()->SetPerPileState(bridgePileId.GetPileIndex(),
                        NKikimrBridge::TClusterState::SYNCHRONIZED);
                }
            }
        }

        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::UpdateClusterStateGeneration(NKikimrBlobStorage::TStorageConfig *config) {
        if (config->HasClusterState() && (!config->GetPrevConfig().GetGeneration() ||
                !NBridge::IsSameClusterState(config->GetClusterState(), config->GetPrevConfig().GetClusterState()))) {
            // update cluster state generation if something has changed
            auto *clusterState = config->MutableClusterState();
            clusterState->SetGeneration(config->GetPrevConfig().GetClusterState().GetGeneration() + 1);
            UpdateClusterStateGuid(clusterState);

            auto *entry = config->MutableClusterStateDetails()->AddUnsyncedHistory();
            entry->MutableClusterState()->CopyFrom(*clusterState);
            for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                TBridgePileId::FromPileIndex(i).CopyToProto(entry, &std::decay_t<decltype(*entry)>::AddUnsyncedPiles);
            }
        }

        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::TrimHistoryEntries(NKikimrBlobStorage::TStorageConfig *config) {
        if (config->HasClusterStateDetails()) {
            auto *details = config->MutableClusterStateDetails();
            auto *history = details->MutableUnsyncedHistory();

            Y_DEBUG_ABORT_UNLESS(config->HasClusterState());
            const auto& clusterState = config->GetClusterState();

            // filter out unsynced piles from config (they become synced when they are the part of the quorum)
            int maxFullySyncedIndex = -1;
            for (int i = 0; i < history->size(); ++i) {
                auto *unsyncedPiles = history->Mutable(i)->MutableUnsyncedPiles();
                for (int j = 0; j < unsyncedPiles->size(); ++j) {
                    const auto unsyncedBridgePileId = TBridgePileId::FromProto(unsyncedPiles,
                        &std::decay_t<decltype(*unsyncedPiles)>::at, j);
                    if (clusterState.PerPileStateSize() <= unsyncedBridgePileId.GetPileIndex()) {
                        Y_DEBUG_ABORT(); // incorrect per pile state
                    } else if (NBridge::PileStateTraits(clusterState.GetPerPileState(
                            unsyncedBridgePileId.GetPileIndex())).RequiresConfigQuorum) {
                        // this pile is a part of quorum we expect to obtain, so it will be config-synced
                        unsyncedPiles->SwapElements(j--, unsyncedPiles->size() - 1);
                        unsyncedPiles->RemoveLast();
                    }
                }
                if (unsyncedPiles->empty()) {
                    maxFullySyncedIndex = i;
                }
            }
            if (maxFullySyncedIndex > 0) {
                history->DeleteSubrange(0, maxFullySyncedIndex);
            }
        }

        return std::nullopt;
    }

    std::optional<TString> TDistributedConfigKeeper::ExtractStateStorageConfig(NKikimrBlobStorage::TStorageConfig *config) {
        // copy bridge info into state storage configs for easier access in replica processors/proxies
        if (config->HasClusterState()) {
            auto fillInBridge = [&](auto *pb) -> std::optional<TString> {
                auto& clusterState = config->GetClusterState();

                // copy cluster state generation
                pb->SetClusterStateGeneration(clusterState.GetGeneration());
                pb->SetClusterStateGuid(clusterState.GetGuid());

                if (!pb->RingGroupsSize() || pb->HasRing()) {
                    return "configuration has Ring field set or no RingGroups";
                }

                auto *groups = pb->MutableRingGroups();
                for (int i = 0, count = groups->size(); i < count; ++i) {
                    auto *group = groups->Mutable(i);
                    if (!group->HasBridgePileId()) {
                        return "bridge pile id is not set for a ring group";
                    } else if (const auto pileId = TBridgePileId::FromProto(group, &std::decay_t<decltype(*group)>::GetBridgePileId);
                            pileId.GetPileIndex() < clusterState.PerPileStateSize()) {
                        using T = NKikimrConfig::TDomainsConfig::TStateStorage;
                        std::optional<T::EPileState> state;
                        if (pileId == TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPrimaryPile)) {
                            state = pileId == TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPromotedPile)
                                ? T::PRIMARY
                                : T::DEMOTED;
                        } else if (pileId == TBridgePileId::FromProto(&clusterState, &NKikimrBridge::TClusterState::GetPromotedPile)) {
                            state = T::PROMOTED;
                        } else {
                            switch (clusterState.GetPerPileState(pileId.GetPileIndex())) {
                                case NKikimrBridge::TClusterState::DISCONNECTED:
                                    state = T::DISCONNECTED;
                                    break;

                                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                                    state = T::NOT_SYNCHRONIZED;
                                    break;

                                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                                    state = T::SYNCHRONIZED;
                                    break;

                                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                                    Y_DEBUG_ABORT("unexpected value");
                            }
                        }
                        if (!state) {
                            return "can't determine correct pile state for ring group";
                        }
                        group->SetPileState(*state);
                    } else {
                        return "bridge pile id is out of bounds";
                    }
                }
                return std::nullopt;
            };

            std::optional<TString> error;
            if (!error && config->HasStateStorageConfig()) {
                error = fillInBridge(config->MutableStateStorageConfig());
            }
            if (!error && config->HasStateStorageBoardConfig()) {
                error = fillInBridge(config->MutableStateStorageBoardConfig());
            }
            if (!error && config->HasSchemeBoardConfig()) {
                error = fillInBridge(config->MutableSchemeBoardConfig());
            }
            return error;
        }

        return std::nullopt;
    }

    void UpdateClusterStateGuid(NKikimrBridge::TClusterState *clusterState) {
        TString buffer;
        const bool success = clusterState->SerializeToString(&buffer);
        Y_ABORT_UNLESS(success);
        clusterState->SetGuid(XXH3_64bits(buffer.data(), buffer.size()));
    }

} // NKikimr::NStorage
