#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxCheckUnsynced : public TTransactionBase<TBlobStorageController> {
    NKikimrBlobStorage::TStorageConfig Config;
    std::optional<TConfigState> State;
    THashSet<TBridgePileId> SyncedPiles;

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
            bool somethingUnsynced = false;

            // find all bridged groups and check their sync states
            State->Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
                if (!groupInfo.BridgeGroupInfo) {
                    return; // this is not a bridge proxy group
                }
                TGroupInfo *info = State->Groups.FindForUpdate(groupId);
                auto *state = info->BridgeGroupInfo->MutableBridgeGroupState();
                if (unsyncedBridgePileId.GetPileIndex() < state->PileSize()) {
                    auto *pile = state->MutablePile(unsyncedBridgePileId.GetPileIndex());
                    const auto targetGroupId = TGroupId::FromProto(pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                    if (pile->GetBecameUnsyncedGeneration() < pss.GetBecameUnsyncedGeneration()) {
                        pile->ClearStage(); // initial stage
                        pile->SetBecameUnsyncedGeneration(pss.GetBecameUnsyncedGeneration());
                        State->GroupContentChanged.insert(groupId);

                        // create entry for syncer, 'cause we want one now
                        const auto [it, inserted] = Self->TargetGroupToSyncerState.try_emplace(targetGroupId, groupId,
                            targetGroupId);
                        if (inserted) {
                            Self->SyncersRequiringAction.PushBack(&it->second);
                        }
                    }
                    if (pile->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                        somethingUnsynced = true;
                    }
                }
            });

            // if there were no unsynced groups remaining, then signal to the NW
            if (!somethingUnsynced) {
                SyncedPiles.insert(unsyncedBridgePileId);
            }
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

        Y_ABORT_UNLESS(Self->CheckingUnsyncedBridgePiles);
        Self->CheckingUnsyncedBridgePiles = !SyncedPiles.empty();
        Self->NotifyBridgeSyncFinishedErrors = false;
        Self->NumPendingBridgeSyncFinishedResponses = SyncedPiles.size();

        for (TBridgePileId bridgePileId : SyncedPiles) {
            NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
            auto *cmd = request.MutableNotifyBridgeSyncFinished();
            cmd->SetGeneration(Config.GetClusterState().GetGeneration());
            bridgePileId.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::SetBridgePileId);
            cmd->SetBSC(true);

            Self->InvokeOnRoot(std::move(request), [self = Self](NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& result) {
                if (result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                    self->NotifyBridgeSyncFinishedErrors = true;
                }
                Y_ABORT_UNLESS(self->NumPendingBridgeSyncFinishedResponses);
                if (!--self->NumPendingBridgeSyncFinishedResponses) {
                    Y_ABORT_UNLESS(self->CheckingUnsyncedBridgePiles);
                    self->CheckingUnsyncedBridgePiles = false;
                    if (self->NotifyBridgeSyncFinishedErrors) {
                        self->CheckUnsyncedBridgePiles();
                    }
                }
            });
        }

        if (Self->RecheckUnsyncedBridgePiles) {
            Self->CheckUnsyncedBridgePiles();
        }

        Self->ProcessSyncers();
    }
};

class TBlobStorageController::TTxUpdateBridgeGroupInfo : public TTransactionBase<TBlobStorageController> {
    const TGroupId GroupId;
    const ui32 GroupGeneration;
    NKikimrBlobStorage::TGroupInfo BridgeGroupInfo;
    const TGroupId TargetGroupId;
    std::optional<TConfigState> State;

public:
    TTxUpdateBridgeGroupInfo(TBlobStorageController *controller, TGroupId groupId, ui32 groupGeneration,
            NKikimrBlobStorage::TGroupInfo&& bridgeGroupInfo, TGroupId targetGroupId)
        : TTransactionBase(controller)
        , GroupId(groupId)
        , GroupGeneration(groupGeneration)
        , BridgeGroupInfo(std::move(bridgeGroupInfo))
        , TargetGroupId(targetGroupId)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_BRIDGE_GROUP_INFO; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());

        if (TGroupInfo *group = State->Groups.FindForUpdate(GroupId); group && group->Generation == GroupGeneration) {
            Y_ABORT_UNLESS(group->BridgeGroupInfo);
            group->BridgeGroupInfo->Swap(&BridgeGroupInfo);
            State->GroupContentChanged.insert(GroupId);
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

        const auto it = Self->TargetGroupToSyncerState.find(TargetGroupId);
        Y_ABORT_UNLESS(it != Self->TargetGroupToSyncerState.end());
        TSyncerState& syncerState = it->second;
        Y_ABORT_UNLESS(syncerState.InCommit);
        syncerState.InCommit = false;
        if (!syncerState.NodeIds) {
            Self->SyncersRequiringAction.PushBack(&syncerState);
        }
        if (TGroupInfo *group = Self->FindGroup(syncerState.BridgeProxyGroupId); group && group->BridgeGroupInfo) {
            for (const auto& pile : group->BridgeGroupInfo->GetBridgeGroupState().GetPile()) {
                if (TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == TargetGroupId &&
                        pile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                    Self->RecheckUnsyncedBridgePiles = true;
                    for (TNodeId nodeId : syncerState.NodeIds | std::views::keys) {
                        const size_t numErased = Self->NodeToSyncerState.erase({nodeId, &syncerState});
                        Y_ABORT_UNLESS(numErased == 1);
                    }
                    Self->TargetGroupToSyncerState.erase(it);
                    break;
                }
            }
        }

        Self->CheckUnsyncedBridgePiles();
        Self->ProcessSyncers();
    }
};

class TBlobStorageController::TTxUpdateBridgeSyncState : public TTransactionBase<TBlobStorageController> {
public:
    struct TChangeStage {};

    struct TRegisterError {
        NKikimrBridge::TGroupState::EStage Stage;
        TString Error;
    };

    struct TGroupStateUpdate {
        TGroupId TargetGroupId;
        std::variant<TChangeStage, TRegisterError> Operation;
    };

private:
    std::vector<TGroupStateUpdate> Updates;
    TInstant Timestamp;

public:
    TTxUpdateBridgeSyncState(TBlobStorageController *controller, std::vector<TGroupStateUpdate>&& updates,
            TInstant timestamp)
        : TTransactionBase(controller)
        , Updates(std::move(updates))
        , Timestamp(timestamp)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_BRIDGE_SYNC_STATE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        auto table = db.Table<Schema::BridgeSyncState>();
        for (auto& update : Updates) {
            auto row = table.Key(update.TargetGroupId.GetRawId());
            std::visit(TOverloaded{
                [&](TChangeStage&) {
                    // when the sync stage gets changed, we forget about any errors that happened before
                    row.Delete();
                    Self->BridgeSyncState.erase(update.TargetGroupId);
                },
                [&](TRegisterError& error) {
                    auto& item = Self->BridgeSyncState[update.TargetGroupId];

                    item.Stage = error.Stage;
                    item.LastError = std::move(error.Error);
                    item.LastErrorTimestamp = Timestamp;

                    if (item.FirstErrorTimestamp == TInstant()) {
                        item.FirstErrorTimestamp = Timestamp;
                        row.Update<Schema::BridgeSyncState::FirstErrorTimestamp>(Timestamp);
                    }

                    ++item.ErrorCount;

                    row.Update(
                        NIceDb::TUpdate<Schema::BridgeSyncState::Stage>(item.Stage),
                        NIceDb::TUpdate<Schema::BridgeSyncState::LastError>(item.LastError),
                        NIceDb::TUpdate<Schema::BridgeSyncState::LastErrorTimestamp>(item.LastErrorTimestamp),
                        NIceDb::TUpdate<Schema::BridgeSyncState::ErrorCount>(item.ErrorCount)
                    );
                }
            }, update.Operation);
            Self->SysViewChangedGroups.insert(update.TargetGroupId);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
    }
};

void TBlobStorageController::CheckUnsyncedBridgePiles() {
    if (!StorageConfig->HasClusterStateDetails() || CheckingUnsyncedBridgePiles) {
        return; // no bridge mode cluster state details available
    }
    const auto& details = StorageConfig->GetClusterStateDetails();
    for (const auto& pss : details.GetPileSyncState()) {
        if (pss.GetUnsyncedBSC()) {
            CheckingUnsyncedBridgePiles = true;
            RecheckUnsyncedBridgePiles = false;
            Execute(std::make_unique<TTxCheckUnsynced>(this, *StorageConfig));
            break;
        }
    }
}

void TBlobStorageController::ApplySyncerState(TNodeId nodeId, const NKikimrBlobStorage::TEvControllerUpdateSyncerState& update,
        TSet<ui32>& groupIdsToRead, bool comprehensive) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR00, "ApplySyncerState", (NodeId, nodeId), (Update, update),
        (Comprehensive, comprehensive));

    // make set of existing target-source tuples to drop unused ones
    THashSet<TSyncerState*> unlistedSyncerState;
    if (comprehensive) {
        for (auto it = NodeToSyncerState.lower_bound({nodeId, nullptr}); it != NodeToSyncerState.end() &&
                std::get<0>(*it) == nodeId; ++it) {
            unlistedSyncerState.insert(std::get<1>(*it));
        }
    }

    bool updateNodeWarden = false; // should we send notification in response
    std::vector<TTxUpdateBridgeSyncState::TGroupStateUpdate> updates;

    // scan update
    for (const auto& syncer : update.GetSyncers()) {
        const auto groupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetBridgeProxyGroupId);
        const ui32 generation = syncer.GetBridgeProxyGroupGeneration();
        const auto sourceGroupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetSourceGroupId);
        const auto targetGroupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetTargetGroupId);

        // find this syncer in our map; if we don't have one, we don't need to do anything about it
        const auto it = TargetGroupToSyncerState.find(targetGroupId);
        if (it == TargetGroupToSyncerState.end()) {
            updateNodeWarden = true; // node warden has reported excessive entry, we should send an update
            continue;
        }
        TSyncerState& syncerState = it->second;

        bool updateGroupInfo = false;
        NKikimrBlobStorage::TGroupInfo bridgeGroupInfo;
        bool correct = false;
        bool staticGroup = false;
        if (const TGroupInfo *group = FindGroup(groupId)) {
            if (generation < group->Generation) {
                updateGroupInfo = true;
            } else if (group->Generation < generation) { // NodeWarden has newer state of group?
                Y_DEBUG_ABORT();
            } else if (group->BridgeGroupInfo && group->BridgeGroupInfo->HasBridgeGroupState()) {
                correct = true;
                bridgeGroupInfo.CopyFrom(*group->BridgeGroupInfo);
            } else { // not a bridged group
                Y_DEBUG_ABORT();
            }
        } else if (const auto it = StaticGroups.find(groupId); it != StaticGroups.end()) {
            const auto& info = it->second.Info;
            const auto& group = info->Group;
            if (generation != info->GroupGeneration) {
                // either NodeWarden, or BSC has obsolete static config generation; we can't accept this report, but
                // we have to handle it anyway
                // FIXME
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR13, "incorrect static group generation reported",
                    (GroupId, groupId),
                    (ReportedGeneration, generation),
                    (KnownGeneration, info->GroupGeneration),
                    (Item, syncer)
                );
            } else if (group && group->HasBridgeGroupState()) {
                correct = true;
                staticGroup = true;
                bridgeGroupInfo.MutableBridgeGroupState()->CopyFrom(group->GetBridgeGroupState());
            } else { // not a bridged static group
                Y_DEBUG_ABORT();
            }
        }
        if (updateGroupInfo) {
            // report fresh groups to the NodeWarden
            groupIdsToRead.insert({groupId.GetRawId(), sourceGroupId.GetRawId(), targetGroupId.GetRawId()});
        }
        if (!correct) {
            updateNodeWarden = true;
            continue;
        }

        // see if we can drop this syncer in favor of already running one
        if (!syncerState.NodeIds.contains(nodeId)) {
            bool dropNewSyncer = false;
            for (auto it = syncerState.NodeIds.begin(); it != syncerState.NodeIds.end(); ) {
                auto& [existingNodeId, item] = *it;
                if (TNodeInfo *node = FindNode(existingNodeId); node && node->ConnectedServerId) {
                    // this node already has a working syncer, delete incoming one
                    dropNewSyncer = true;
                    ++it;
                } else {
                    // we have to remove existing syncer; don't need to update it, 'cause node is not connected/working
                    syncerState.NodeIds.erase(it++);
                    NodeToSyncerState.erase({existingNodeId, &syncerState});
                }
            }
            if (dropNewSyncer) {
                updateNodeWarden = true;
                continue;
            }
        }

        unlistedSyncerState.erase(&syncerState);
        auto& perNodeInfo = syncerState.NodeIds[nodeId];
        perNodeInfo.SourceGroupId = sourceGroupId;
        NodeToSyncerState.emplace(nodeId, &syncerState);
        syncerState.Unlink();

        // update any progress
        if (syncer.HasBytesDone() || syncer.HasBytesTotal() || syncer.HasBytesError() ||
                syncer.HasBlobsDone() || syncer.HasBlobsTotal() || syncer.HasBlobsError()) {
            auto& progress = perNodeInfo.Progress;
#define FETCH_METRIC(NAME) \
            if (syncer.Has##NAME()) { \
                progress.NAME = syncer.Get##NAME(); \
            }

            FETCH_METRIC(BytesDone)
            FETCH_METRIC(BytesTotal)
            FETCH_METRIC(BytesError)
            FETCH_METRIC(BlobsDone)
            FETCH_METRIC(BlobsTotal)
            FETCH_METRIC(BlobsError)

#undef FETCH_METRIC
        }

        if (syncer.GetFinished()) {
            // syncer is finished, so we remove it from working set
            size_t numErased = NodeToSyncerState.erase({nodeId, &syncerState});
            Y_ABORT_UNLESS(numErased == 1);
            numErased = syncerState.NodeIds.erase(nodeId);
            Y_ABORT_UNLESS(numErased == 1);
            if (!syncerState.NodeIds && !syncerState.InCommit) {
                SyncersRequiringAction.PushBack(&syncerState);
            }
            updateNodeWarden = true;

            if (syncer.HasErrorReason()) {
                NKikimrBridge::TGroupState::EStage stage;
                for (const auto& pile : bridgeGroupInfo.GetBridgeGroupState().GetPile()) {
                    if (TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == targetGroupId) {
                        stage = pile.GetStage();
                        break;
                    }
                }
                updates.emplace_back(targetGroupId, TTxUpdateBridgeSyncState::TRegisterError{
                    .Stage = stage,
                    .Error = syncer.GetErrorReason(),
                });
                continue;
            }
            if (syncerState.InCommit) {
                // this is just a repeat
                continue;
            }

            // let's update syncer state (switch it to next one)
            auto *state = bridgeGroupInfo.MutableBridgeGroupState();
            bool found = false;
            for (auto& pile : *state->MutablePile()) {
                if (TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == targetGroupId) {
                    switch (pile.GetStage()) {
                        case NKikimrBridge::TGroupState::BLOCKS:
                        case NKikimrBridge::TGroupState::WRITE_KEEP:
                        case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP:
                        case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA:
                            pile.SetStage(static_cast<NKikimrBridge::TGroupState::EStage>(pile.GetStage() + 1));
                            break;

                        case NKikimrBridge::TGroupState::SYNCED:
                            break; // already synced, syncer just did nothing

                        default:
                            Y_DEBUG_ABORT_S("incorrect pile Stage# " << NKikimrBridge::TGroupState::EStage_Name(pile.GetStage())
                                << " GroupId# " << targetGroupId);
                    }
                    found = true;
                }
            }
            Y_DEBUG_ABORT_UNLESS(found);

            syncerState.InCommit = true; // we are committing syncer state right now
            syncerState.Unlink();

            updates.emplace_back(targetGroupId, TTxUpdateBridgeSyncState::TChangeStage{});

            if (staticGroup) {
                NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
                auto *cmd = request.MutableUpdateBridgeGroupInfo();
                groupId.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::SetGroupId);
                cmd->SetGroupGeneration(generation);
                cmd->MutableBridgeGroupInfo()->Swap(&bridgeGroupInfo);
                InvokeOnRoot(std::move(request), [=](NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& result) {
                    if (result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                        Y_DEBUG_ABORT("UpdateBridgeGroupInfo has unexpectedly failed");
                        STLOG(PRI_ERROR, BS_CONTROLLER, BSCBR08, "UpdateBridgeGroupInfo has unexpectedly failed",
                            (Result, result));
                    }
                    if (const auto it = TargetGroupToSyncerState.find(targetGroupId); it != TargetGroupToSyncerState.end()) {
                        TSyncerState& syncerState = it->second;
                        Y_ABORT_UNLESS(syncerState.InCommit);
                        syncerState.InCommit = false;
                        if (!syncerState.NodeIds) {
                            SyncersRequiringAction.PushBack(&syncerState);
                        }
                    }
                });
            } else {
                Execute(std::make_unique<TTxUpdateBridgeGroupInfo>(this, groupId, generation,
                    std::move(bridgeGroupInfo), targetGroupId));
            }
        }
    }

    // delete missing syncers
    for (TSyncerState *syncerState : unlistedSyncerState) {
        size_t numErased = syncerState->NodeIds.erase(nodeId);
        Y_ABORT_UNLESS(numErased == 1);
        numErased = NodeToSyncerState.erase({nodeId, syncerState});
        Y_ABORT_UNLESS(numErased == 1);
        if (!syncerState->NodeIds && !syncerState->InCommit) {
            SyncersRequiringAction.PushBack(syncerState);
        }
    }

    if (!updates.empty()) {
        Execute(std::make_unique<TTxUpdateBridgeSyncState>(this, std::move(updates), TActivationContext::Now()));
    }

    THashSet<TNodeId> nodesToUpdate;
    if (updateNodeWarden) {
        nodesToUpdate.insert(nodeId);
    }
    ProcessSyncers(std::move(nodesToUpdate));
}

void TBlobStorageController::CheckSyncerDisconnectedNodes() {
    const auto now = TActivationContext::Monotonic();

    for (auto it = NodeToSyncerState.begin(); it != NodeToSyncerState.end(); ) {
        const auto& [nodeId, syncerState] = *it;
        const TNodeInfo *node = FindNode(nodeId);
        Y_ABORT_UNLESS(node);

        // find all items for this nodeId
        auto beginIt = it;
        while (it != NodeToSyncerState.end() && std::get<0>(*it) == nodeId) {
            ++it;
        }

        if (node->DisconnectedTimestampMono <= now - DisconnectedSyncerReactionTime) {
            for (auto jt = beginIt; jt != it; ++jt) {
                const auto& [nodeId, syncerState] = *jt;
                const size_t numErased = syncerState->NodeIds.erase(nodeId);
                Y_ABORT_UNLESS(numErased == 1);
                if (!syncerState->NodeIds && !syncerState->InCommit) {
                    SyncersRequiringAction.PushBack(syncerState);
                }
            }
            NodeToSyncerState.erase(beginIt, it);
        }
    }

    TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvPrivate::EvCheckSyncerDisconnectedNodes, 0,
        SelfId(), {}, nullptr, 0));
}

void TBlobStorageController::ProcessSyncers(THashSet<TNodeId> nodesToUpdate) {
    if (nodesToUpdate.empty() && TActivationContext::Monotonic() < LoadedAt + TDuration::Seconds(10)) {
        return; // give nodes some time to connect to BSC
    }

    for (auto it = SyncersRequiringAction.begin(); it != SyncersRequiringAction.end(); ) {
        TSyncerState& syncerState = *it++;

        if (syncerState.InCommit || syncerState.NodeIds) { // can't happen
            Y_FAIL_S("InCommit# " << syncerState.InCommit
                << " NodeIds# " << FormatList(syncerState.NodeIds | std::views::keys));
        }

        const NKikimrBlobStorage::TGroupInfo *bridgeGroupInfo = nullptr;
        if (const TGroupInfo *group = FindGroup(syncerState.BridgeProxyGroupId)) {
            Y_ABORT_UNLESS(group->BridgeGroupInfo);
            Y_ABORT_UNLESS(group->BridgeGroupInfo->HasBridgeGroupState());
            bridgeGroupInfo = &group->BridgeGroupInfo.value();
        } else if (const auto it = StaticGroups.find(syncerState.BridgeProxyGroupId); it != StaticGroups.end()) {
            const auto& group = it->second.Info->Group;
            Y_ABORT_UNLESS(group);
            Y_ABORT_UNLESS(group->HasBridgeGroupState());
            bridgeGroupInfo = &group.value();
        } else {
            Y_DEBUG_ABORT();
            continue;
        }

        const auto& state = bridgeGroupInfo->GetBridgeGroupState();
        for (size_t pileIndex = 0; pileIndex < state.PileSize(); ++pileIndex) {
            if (BridgeInfo->GetPile(TBridgePileId::FromPileIndex(pileIndex))->State != NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2) {
                continue; // can't start syncing yet
            }

            const auto& pile = state.GetPile(pileIndex);
            const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (targetGroupId != syncerState.TargetGroupId) {
                continue;
            }
            if (pile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                continue; // this group is synced
            }

            // pick the pile to sync from
            std::optional<TGroupId> sourceGroupId;
            for (const auto& pile : state.GetPile()) {
                if (pile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                    sourceGroupId.emplace(TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId));
                    break;
                }
            }
            if (!sourceGroupId) {
                // can't find origin group
                Y_DEBUG_ABORT();
                continue;
            }

            // find available nodes
            std::vector<TNodeId> nodes;
            if (const TGroupInfo *group = FindGroup(targetGroupId)) {
                for (const auto& vdisk : group->VDisksInGroup) {
                    nodes.push_back(vdisk->VSlotId.NodeId);
                }
            } else if (const auto it = StaticGroups.find(targetGroupId); it != StaticGroups.end()) {
                for (const auto& actorId : it->second.Info->GetDynamicInfo().ServiceIdForOrderNumber) {
                    nodes.push_back(actorId.NodeId());
                }
            } else {
                Y_ABORT();
            }
            if (nodes.empty()) {
                for (const auto& [nodeId, node] : Nodes) {
                    nodes.push_back(nodeId);
                }
            }
            auto pred = [&](TNodeId nodeId) {
                const TNodeInfo *nodeInfo = FindNode(nodeId);
                return !nodeInfo || !nodeInfo->ConnectedServerId;
            };
            std::erase_if(nodes, pred);
            if (nodes.empty()) {
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR02, "ProcessSyncers: no nodes to start syncer at",
                    (TargetGroupId, targetGroupId), (SourceGroupId, *sourceGroupId));
                continue;
            }

            // pick random one
            const size_t index = RandomNumber(nodes.size());
            const TNodeId nodeId = nodes[index];

            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR03, "ProcessSyncers: starting syncer",
                (TargetGroupId, targetGroupId), (SourceGroupId, *sourceGroupId), (Nodes, nodes), (NodeId, nodeId),
                (GroupPile, pile));

            syncerState.NodeIds[nodeId].SourceGroupId = *sourceGroupId;
            NodeToSyncerState.emplace(nodeId, &syncerState);
            nodesToUpdate.insert(nodeId);
            syncerState.Unlink();
        }
    }

    for (TNodeId nodeId : nodesToUpdate) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        TSet<ui32> groupIdsToRead;
        SerializeSyncers(nodeId, &ev->Record, groupIdsToRead);
        ReadGroups(groupIdsToRead, false, ev.get(), nodeId);
        SendToWarden(nodeId, std::move(ev), 0);
    }
}

void TBlobStorageController::SerializeSyncers(TNodeId nodeId, NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate *update,
        TSet<ui32>& groupIdsToRead) {
    for (auto it = NodeToSyncerState.lower_bound({nodeId, nullptr}); it != NodeToSyncerState.end() && std::get<0>(*it) == nodeId; ++it) {
        const auto& [nodeId, syncerState] = *it;

        const auto jt = syncerState->NodeIds.find(nodeId);
        Y_ABORT_UNLESS(jt != syncerState->NodeIds.end());
        const TGroupId sourceGroupId = jt->second.SourceGroupId;

        auto *syncer = update->AddSyncers();
        using T = std::decay_t<decltype(*syncer)>;

        syncerState->BridgeProxyGroupId.CopyToProto(syncer, &T::SetBridgeProxyGroupId);
        sourceGroupId.CopyToProto(syncer, &T::SetSourceGroupId);
        syncerState->TargetGroupId.CopyToProto(syncer, &T::SetTargetGroupId);

        if (const TGroupInfo *group = FindGroup(syncerState->BridgeProxyGroupId)) {
            syncer->SetBridgeProxyGroupGeneration(group->Generation);
            groupIdsToRead.insert({syncer->GetBridgeProxyGroupId(), syncer->GetSourceGroupId(), syncer->GetTargetGroupId()});
        } else if (const auto it = StaticGroups.find(syncerState->BridgeProxyGroupId); it != StaticGroups.end()) {
            syncer->SetBridgeProxyGroupGeneration(it->second.Info->GroupGeneration);
        } else {
            Y_DEBUG_ABORT();
            continue;
        }
    }
    update->SetUpdateSyncers(true);
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerUpdateSyncerState::TPtr ev) {
    const TNodeId nodeId = ev->Sender.NodeId();
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR04, "TEvControllerUpdateSyncerState", (NodeId, nodeId), (Msg, ev->Get()->Record));
    TSet<ui32> groupIdsToRead;
    ApplySyncerState(nodeId, ev->Get()->Record, groupIdsToRead, false);
    if (groupIdsToRead) {
        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        ReadGroups(groupIdsToRead, false, update.get(), nodeId);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR05, "TEvControllerUpdateSyncerState: sending update", (Msg, update->Record));
        SendToWarden(nodeId, std::move(update), 0);
    }
}

void TBlobStorageController::RenderBridge(IOutputStream& out) {
    RenderHeader(out);

    HTML(out) {
        TAG(TH3) {
            out << "Working syncers";
        }

        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Bridge proxy group id"; }
                    TABLEH() { out << "Target group id"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "Node id"; }
                    TABLEH() { out << "Source group id"; }
                    TABLEH() { out << "Progress"; }
                    TABLEH() { out << "Bytes"; }
                    TABLEH() { out << "Blobs"; }
                }
            }
            TABLEBODY() {
                for (auto& [targetGroupId, syncerState] : TargetGroupToSyncerState) {
                    TString state;
                    auto scanPiles = [&](auto& info) {
                        if (!info) {
                            return;
                        }
                        for (const auto& pile : info->GetBridgeGroupState().GetPile()) {
                            if (TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId) == targetGroupId) {
                                state = NKikimrBridge::TGroupState::EStage_Name(pile.GetStage());
                                break;
                            }
                        }
                    };
                    if (const TGroupInfo *group = FindGroup(syncerState.BridgeProxyGroupId)) {
                        scanPiles(group->BridgeGroupInfo);
                    } else if (const auto it = StaticGroups.find(syncerState.BridgeProxyGroupId); it != StaticGroups.end()) {
                        scanPiles(it->second.Info->Group);
                    }

                    if (syncerState.NodeIds.empty()) {
                        TABLER() {
                            TABLED() { out << syncerState.BridgeProxyGroupId; }
                            TABLED() { out << targetGroupId; }
                            TABLED() { out << state; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                        }
                    }
                    for (const auto& [nodeId, perNodeInfo] : syncerState.NodeIds) {
                        TABLER() {
                            TABLED() { out << syncerState.BridgeProxyGroupId; }
                            TABLED() { out << targetGroupId; }
                            TABLED() { out << state; }
                            TABLED() {
                                out << "<a href='/node/" << nodeId << "/actors/nodewarden#syncer-" << targetGroupId << "'>" << nodeId << "</a>";
                            }
                            TABLED() {
                                out << perNodeInfo.SourceGroupId;
                            }

                            auto& progress = perNodeInfo.Progress;
                            TABLED() {
                                if (progress.BytesTotal) {
                                    const int percent = 10'000 * progress.BytesDone / progress.BytesTotal;
                                    out << Sprintf("%d.%02d%%", percent / 100, percent % 100);
                                }
                            }
                            TABLED() {
                                static const char *bytesSuffixes[] = {"B", "KiB", "MiB", "GiB", nullptr};
                                FormatHumanReadable(out, progress.BytesDone, 1024, 1, bytesSuffixes);
                                out << '/';
                                FormatHumanReadable(out, progress.BytesTotal, 1024, 1, bytesSuffixes);
                                out << '(';
                                FormatHumanReadable(out, progress.BytesError, 1024, 1, bytesSuffixes);
                                out << ')';
                            }
                            TABLED() {
                                static const char *blobsSuffixes[] = {"", "K", "M", nullptr};
                                FormatHumanReadable(out, progress.BlobsDone, 1000, 1, blobsSuffixes);
                                out << '/';
                                FormatHumanReadable(out, progress.BlobsTotal, 1000, 1, blobsSuffixes);
                                out << '(';
                                FormatHumanReadable(out, progress.BlobsError, 1000, 1, blobsSuffixes);
                                out << ')';
                            }
                        }
                    }
                }
            }
        }
    }
}

void TBlobStorageController::ApplyStaticGroupUpdateForSyncers(std::map<TGroupId, TStaticGroupInfo>& prevStaticGroups) {
    // make a list of all currently unsynced groups
    THashSet<TGroupId> targetGroupsToDelete;
    for (const auto& [groupId, group] : prevStaticGroups) {
        if (group.Info && group.Info->Group && group.Info->Group->HasBridgeGroupState()) {
            const auto& bridgeGroupState = group.Info->Group->GetBridgeGroupState();
            for (const auto& pile : bridgeGroupState.GetPile()) {
                const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                    Y_ABORT_UNLESS(TargetGroupToSyncerState.contains(targetGroupId));
                    const bool inserted = targetGroupsToDelete.insert(targetGroupId).second;
                    Y_ABORT_UNLESS(inserted);
                } else {
                    Y_ABORT_UNLESS(!TargetGroupToSyncerState.contains(targetGroupId));
                }
            }
        }
    }

    // process fresh group list
    THashSet<TNodeId> nodesToUpdate;
    for (const auto& group : StorageConfig->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
        if (group.HasBridgeGroupState()) {
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            const auto& bridgeGroupState = group.GetBridgeGroupState();
            for (const auto& pile : bridgeGroupState.GetPile()) {
                if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                    const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                    const auto [it, inserted] = TargetGroupToSyncerState.try_emplace(targetGroupId, groupId,
                        targetGroupId);
                    if (inserted) {
                        SyncersRequiringAction.PushBack(&it->second);
                    } else {
                        const size_t n = targetGroupsToDelete.erase(targetGroupId);
                        Y_ABORT_UNLESS(n == 1);

                        const auto prevIt = prevStaticGroups.find(it->second.BridgeProxyGroupId);
                        const auto curIt = StaticGroups.find(it->second.BridgeProxyGroupId);
                        if (prevIt != prevStaticGroups.end() && curIt != StaticGroups.end() &&
                                prevIt->second.Info->GroupGeneration < curIt->second.Info->GroupGeneration) {
                            // bridge proxy group generation has changed and hence we have to issue new command with
                            // the relevant generation
                            for (TNodeId nodeId : it->second.NodeIds | std::views::keys) {
                                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR01, "refreshing syncers with obsolete static group config",
                                    (NodeId, nodeId), (Pile, pile), (BridgeGroupState, bridgeGroupState));
                                nodesToUpdate.insert(nodeId);
                            }
                        }
                    }
                }
            }
        }
    }

    // remove obsolete entries
    for (TGroupId targetGroupId : targetGroupsToDelete) {
        const auto it = TargetGroupToSyncerState.find(targetGroupId);
        Y_ABORT_UNLESS(it != TargetGroupToSyncerState.end());
        TSyncerState& syncerState = it->second;
        for (TNodeId nodeId : syncerState.NodeIds | std::views::keys) {
            const size_t n = NodeToSyncerState.erase({nodeId, &syncerState});
            Y_ABORT_UNLESS(n == 1);
        }
        TargetGroupToSyncerState.erase(it);
    }

    // kick syncers for changed groups
    for (TNodeId nodeId : nodesToUpdate) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        TSet<ui32> groupIdsToRead;
        SerializeSyncers(nodeId, &ev->Record, groupIdsToRead);
        ReadGroups(groupIdsToRead, false, ev.get(), nodeId);
        SendToWarden(nodeId, std::move(ev), 0);
    }
}

void TBlobStorageController::CommitSyncerUpdates(TConfigState& state, TTransactionContext& txc) {
    for (const auto& [base, overlay] : state.Groups.Diff()) {
        if (base && !overlay->second) { // deleted group
            const TGroupId groupId = base->first;
            if (const auto it = TargetGroupToSyncerState.find(groupId); it != TargetGroupToSyncerState.end()) {
                TSyncerState& syncerState = it->second;
                for (TNodeId nodeId : syncerState.NodeIds | std::views::keys) {
                    const size_t n = NodeToSyncerState.erase({nodeId, &syncerState});
                    Y_ABORT_UNLESS(n == 1);
                }
                TargetGroupToSyncerState.erase(it);
            }
            if (BridgeSyncState.erase(groupId)) {
                NIceDb::TNiceDb(txc.DB).Table<Schema::BridgeSyncState>().Key(groupId.GetRawId()).Delete();
            }
        }
    }
}

} // NKikimr::NBsController
