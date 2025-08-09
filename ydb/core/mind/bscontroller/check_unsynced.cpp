#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxCheckUnsynced : public TTransactionBase<TBlobStorageController> {
    NKikimrBlobStorage::TStorageConfig Config;
    std::optional<TConfigState> State;

public:
    TTxCheckUnsynced(TBlobStorageController *controller, const NKikimrBlobStorage::TStorageConfig& config)
        : TTransactionBase(controller)
        , Config(config)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_CHECK_UNSYNCED; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());

        Y_ABORT_UNLESS(Config.HasClusterStateDetails());
        for (const auto& pss : Config.GetClusterStateDetails().GetPileSyncState()) {
            if (!pss.GetUnsyncedBSC()) {
                continue;
            }
            const auto unsyncedBridgePileId = TBridgePileId::FromProto(&pss, &std::decay_t<decltype(pss)>::GetBridgePileId);

            // find all bridged groups and check their sync states
            State->Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
                if (!groupInfo.BridgeGroupInfo) {
                    return; // this is not a bridge proxy group
                }
                TGroupInfo *info = State->Groups.FindForUpdate(groupId);
                auto *state = info->BridgeGroupInfo->MutableBridgeGroupState();
                if (unsyncedBridgePileId.GetPileIndex() < state->PileSize()) {
                    auto *pile = state->MutablePile(unsyncedBridgePileId.GetPileIndex());
                    if (pile->GetBecameUnsyncedGeneration() < pss.GetBecameUnsyncedGeneration()) {
                        pile->SetStage(NKikimrBridge::TGroupState::BLOCKS); // initial stage
                        pile->SetBecameUnsyncedGeneration(pss.GetBecameUnsyncedGeneration());
                    }
                }
            });
        }

        if (TString error; State->Changed() && !Self->CommitConfigUpdates(*State, true, true, true, txc, &error)) {
            State->Rollback();
            State.reset();
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
        }
    }
};

void TBlobStorageController::CheckUnsyncedBridgePiles() {
    if (!StorageConfig->HasClusterStateDetails()) {
        return; // no bridge mode cluster state details available
    }
    const auto& details = StorageConfig->GetClusterStateDetails();
    for (const auto& pss : details.GetPileSyncState()) {
        if (pss.GetUnsyncedBSC()) {
            Execute(std::make_unique<TTxCheckUnsynced>(this, *StorageConfig));
            return;
        }
    }
}

void TBlobStorageController::ApplySyncerState(TNodeId nodeId, const NKikimrBlobStorage::TEvControllerUpdateSyncerState& update,
        TSet<ui32>& groupIdsToRead) {
    // make set of existing target-source tuples to drop unused ones
    THashSet<std::tuple<TGroupId, TGroupId>> targetSourceToDelete;
    for (auto it = SyncersNodeTargetSource.lower_bound(std::make_tuple(nodeId, TGroupId::Min(), TGroupId::Min()));
            it != SyncersNodeTargetSource.end() && std::get<0>(*it) == nodeId; ++it) {
        const auto& [nodeId, targetGroupId, sourceGroupId] = *it;
        targetSourceToDelete.emplace(targetGroupId, sourceGroupId);
    }

    // scan update
    for (const auto& syncer : update.GetSyncers()) {
        const auto groupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetBridgeProxyGroupId);
        const ui32 generation = syncer.GetBridgeProxyGroupGeneration();
        const auto sourceGroupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetSourceGroupId);
        const auto targetGroupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetTargetGroupId);

        bool updateGroupInfo = false;
        bool correct = false;
        if (TGroupID(groupId).ConfigurationType() == EGroupConfigurationType::Static) {
            if (const auto it = StaticGroups.find(groupId); it != StaticGroups.end()) {
                auto& info = it->second.Info;
                if (generation < info->GroupGeneration) {
                    updateGroupInfo = true;
                } else if (info->GroupGeneration < generation) {
                    // some kind of race? BSC's generation of static group is less than reported one, can't validate
                } else {
                    correct = true;
                }
            }
        } else {
            if (const TGroupInfo *group = FindGroup(groupId)) {
                if (generation < group->Generation) {
                    updateGroupInfo = true;
                } else if (generation < group->Generation) {
                    Y_DEBUG_ABORT();
                } else {
                    correct = true;
                }
            }
        }
        if (updateGroupInfo) {
            groupIdsToRead.insert(groupId.GetRawId());
            groupIdsToRead.insert(sourceGroupId.GetRawId());
            groupIdsToRead.insert(targetGroupId.GetRawId());
        }
        if (!correct) {
            continue;
        }

        if (syncer.GetFinished()) {
            //...
            continue;
        }

        SyncersNodeTargetSource.emplace(nodeId, targetGroupId, sourceGroupId);
        SyncersTargetNodeSource.emplace(targetGroupId, nodeId, sourceGroupId);
        targetSourceToDelete.erase(std::make_tuple(targetGroupId, sourceGroupId));
    }

    // delete missing syncers
    for (const auto& [targetGroupId, sourceGroupId] : targetSourceToDelete) {
        SyncersNodeTargetSource.erase(std::make_tuple(nodeId, targetGroupId, sourceGroupId));
        SyncersTargetNodeSource.erase(std::make_tuple(targetGroupId, nodeId, sourceGroupId));
    }
}

void TBlobStorageController::CheckSyncerDisconnectedNodes() {
    const auto now = TActivationContext::Monotonic();
    auto it = SyncersNodeTargetSource.begin();
    while (it != SyncersNodeTargetSource.end()) {
        const auto& [nodeId, targetGroupId, sourceGroupId] = *it;
        const TNodeInfo *node = FindNode(nodeId);
        Y_ABORT_UNLESS(node);
        if (node->DisconnectedTimestampMono <= now - DisconnectedSyncerReactionTime) {
            // delete all items for this node
            const auto first = it;
            while (it != SyncersNodeTargetSource.end() && std::get<0>(*it) == nodeId) {
                const auto& [nodeId, targetGroupId, sourceGroupId] = *it++;
                const size_t n = SyncersTargetNodeSource.erase(std::make_tuple(targetGroupId, nodeId, sourceGroupId));
                Y_ABORT_UNLESS(n == 1);
            }
            SyncersNodeTargetSource.erase(first, it);
        } else {
            // just skip this node
            while (it != SyncersNodeTargetSource.end() && std::get<0>(*it) == nodeId) {
                ++it;
            }
        }
    }

    StartRequiredSyncers();

    TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvPrivate::EvCheckSyncerDisconnectedNodes, 0,
        SelfId(), {}, nullptr, 0));
}

void TBlobStorageController::StartRequiredSyncers() {
    if (TActivationContext::Monotonic() < LoadedAt + TDuration::Seconds(10)) {
        return; // give nodes some time to connect to BSC
    }

    auto processGroup = [this](TGroupId groupId, const auto& bridgeGroupInfo) {
        for (const auto& pile : bridgeGroupInfo.GetBridgeGroupState().GetPile()) {
            if (pile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                continue; // everything is okay with this group
            }
            // we need a syncer
            const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            bool needToStartSyncer = true;
            for (auto it = SyncersTargetNodeSource.lower_bound(std::make_tuple(targetGroupId, TNodeId(), TGroupId()));
                    it != SyncersTargetNodeSource.end() && std::get<0>(*it) == targetGroupId; ++it) {
                const auto& [targetGroupId, nodeId, sourceGroupId] = *it;
                // check if this source group is suitable for syncing
                for (const auto& sourcePile : bridgeGroupInfo.GetBridgeGroupState().GetPile()) {
                    if (TGroupId::FromProto(&sourcePile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == sourceGroupId) {
                        if (sourcePile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                            needToStartSyncer = false;
                        }
                        break;
                    }
                }
            }
            if (!needToStartSyncer) {
                continue; // don't do anything else
            }
            // pick a node for syncer
            (void)groupId;
        }
    };

    for (const auto& [groupId, group] : GroupMap) {
        if (group->BridgeGroupInfo) {
            processGroup(groupId, *group->BridgeGroupInfo);
        }
    }
    for (const auto& [groupId, group] : StaticGroups) {
        if (group.Info->Group->HasBridgeGroupState()) {
            processGroup(groupId, *group.Info->Group);
        }
    }
}

} // NKikimr::NBsController
