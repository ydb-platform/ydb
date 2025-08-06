#include "distconf.h"
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NStorage {

    bool TDistributedConfigKeeper::UpdateBridgeConfig(NKikimrBlobStorage::TStorageConfig *config) {
        bool changes = false;

        if (config->HasClusterStateDetails()) {
            auto *details = config->MutableClusterStateDetails();
            auto *history = details->MutableUnsyncedHistory();

            Y_DEBUG_ABORT_UNLESS(config->HasClusterState());
            auto *clusterState = config->MutableClusterState();

            // switch unsynced piles (NOT_SYNCHRONIZED_1) to NOT_SYNCHRONIZED_2 when connected
            bool clusterStateUpdated = false;
            for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                if (clusterState->GetPerPileState(i) == NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1 &&
                        ConnectedUnsyncedPiles.contains(TBridgePileId::FromPileIndex(i))) {
                    clusterState->SetPerPileState(i, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
                    clusterStateUpdated = true;

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
                        pile->SetStage(NKikimrBridge::TGroupState::BLOCKS);
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
            if (clusterStateUpdated) {
                clusterState->SetGeneration(clusterState->GetGeneration() + 1);
                UpdateClusterStateGuid(clusterState);

                auto *item = history->Add();
                item->MutableClusterState()->CopyFrom(*clusterState);
                for (size_t i = 0; i < clusterState->PerPileStateSize(); ++i) {
                    TBridgePileId::FromPileIndex(i).CopyToProto(item, &std::decay_t<decltype(*item)>::AddUnsyncedPiles);
                }

                changes = true;
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
        state->SetGeneration(1);
        auto *piles = state->MutablePerPileState();
        for (size_t i = 0; i < cfg.BridgeConfig->PilesSize(); ++i) {
            piles->Add(NKikimrBridge::TClusterState::SYNCHRONIZED);
        }
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPromotedPile);

        auto *details = config->MutableClusterStateDetails();
        auto *entry = details->AddUnsyncedHistory();
        entry->MutableClusterState()->CopyFrom(*state);
        for (int i = 0; i < piles->size(); ++i) {
            TBridgePileId::FromPileIndex(i).CopyToProto(entry, &std::decay_t<decltype(*entry)>::AddUnsyncedPiles);
        }
    }

    bool TDistributedConfigKeeper::CheckBridgePeerRevPush(const NKikimrBlobStorage::TStorageConfig& peerConfig,
            ui32 senderNodeId) {
        (void)peerConfig;
        (void)senderNodeId;
        return true;
    }

    void UpdateClusterStateGuid(NKikimrBridge::TClusterState *clusterState) {
        TString buffer;
        const bool success = clusterState->SerializeToString(&buffer);
        Y_ABORT_UNLESS(success);
        clusterState->SetGuid(XXH3_64bits(buffer.data(), buffer.size()));
    }

} // NKikimr::NStorage
