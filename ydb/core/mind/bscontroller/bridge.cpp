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
                    if (pile->GetBecameUnsyncedGeneration() < pss.GetBecameUnsyncedGeneration()) {
                        pile->ClearStage(); // initial stage
                        pile->SetBecameUnsyncedGeneration(pss.GetBecameUnsyncedGeneration());
                        State->GroupContentChanged.insert(groupId);
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
        for (TBridgePileId bridgePileId : SyncedPiles) {
            NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
            auto *cmd = request.MutableNotifyBridgeSyncFinished();
            cmd->SetGeneration(Config.GetClusterState().GetGeneration());
            bridgePileId.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::SetBridgePileId);
            cmd->SetBSC(true);
            Self->InvokeOnRoot(std::move(request), [](auto&) {});
        }
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
        Self->TargetGroupsInCommit.erase(TargetGroupId);
        Self->CheckUnsyncedBridgePiles();
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
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR00, "ApplySyncerState", (NodeId, nodeId), (Update, update));

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
        NKikimrBlobStorage::TGroupInfo bridgeGroupInfo;
        bool correct = false;
        bool staticGroup = false;
        if (const TGroupInfo *group = FindGroup(groupId)) {
            if (generation < group->Generation) {
                updateGroupInfo = true;
            } else if (generation < group->Generation) {
                Y_DEBUG_ABORT();
            } else if (group->BridgeGroupInfo && group->BridgeGroupInfo->HasBridgeGroupState()) {
                correct = true;
                staticGroup = false;
                bridgeGroupInfo.CopyFrom(*group->BridgeGroupInfo);
            } else {
                Y_DEBUG_ABORT();
            }
        } else if (const auto it = StaticGroups.find(groupId); it != StaticGroups.end()) {
            const auto& info = it->second.Info;
            const auto& group = info->Group;
            if (generation < info->GroupGeneration) {
                // can't really update group info for static group this way
            } else if (info->GroupGeneration < generation) {
                // some kind of race? BSC's generation of static group is less than reported one, can't validate
            } else if (group && group->HasBridgeGroupState()) {
                correct = true;
                staticGroup = true;
                bridgeGroupInfo.MutableBridgeGroupState()->CopyFrom(group->GetBridgeGroupState());
            } else {
                Y_DEBUG_ABORT();
            }
        }
        if (updateGroupInfo && !staticGroup) {
            groupIdsToRead.insert({groupId.GetRawId(), sourceGroupId.GetRawId(), targetGroupId.GetRawId()});
        }
        if (!correct) {
            continue;
        }

        if (syncer.GetFinished()) {
            if (syncer.HasErrorReason() || TargetGroupsInCommit.contains(targetGroupId)) {
                continue;
            }
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

                        default:
                            Y_DEBUG_ABORT_S("incorrect pile Stage# " << NKikimrBridge::TGroupState::EStage_Name(pile.GetStage())
                                << " GroupId# " << targetGroupId);
                    }
                    found = true;
                }
            }
            Y_DEBUG_ABORT_UNLESS(found);
            TargetGroupsInCommit.insert(targetGroupId);
            if (staticGroup) {
                NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
                auto *cmd = request.MutableUpdateBridgeGroupInfo();
                groupId.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::SetGroupId);
                cmd->SetGroupGeneration(generation);
                cmd->MutableBridgeGroupInfo()->Swap(&bridgeGroupInfo);
                InvokeOnRoot(std::move(request), [=](NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& /*result*/) {
                    TargetGroupsInCommit.erase(targetGroupId);
                });
            } else {
                Execute(std::make_unique<TTxUpdateBridgeGroupInfo>(this, groupId, generation,
                    std::move(bridgeGroupInfo), targetGroupId));
            }
            // we will resume syncer when commit is done
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

    auto getGroupStateByTargetGroup = [&](TGroupId groupId) {
        if (const TGroupInfo *group = FindGroup(groupId)) {
            Y_ABORT_UNLESS(group->BridgeProxyGroupId);
            const TGroupInfo *bridgeGroup = FindGroup(*group->BridgeProxyGroupId);
            Y_ABORT_UNLESS(bridgeGroup->BridgeGroupInfo);
            Y_ABORT_UNLESS(bridgeGroup->BridgeGroupInfo->HasBridgeGroupState());
            return &bridgeGroup->BridgeGroupInfo->GetBridgeGroupState();
        } else if (const auto it = StaticGroups.find(groupId); it != StaticGroups.end()) {
            const auto& group = it->second.Info->Group;
            Y_ABORT_UNLESS(group);
            Y_ABORT_UNLESS(group->HasBridgeProxyGroupId());
            const auto jt = StaticGroups.find(TGroupId::FromProto(&group.value(),
                &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId));
            Y_ABORT_UNLESS(jt != StaticGroups.end());
            const auto& bridgeGroup = jt->second.Info->Group;
            Y_ABORT_UNLESS(bridgeGroup);
            Y_ABORT_UNLESS(bridgeGroup->HasBridgeGroupState());
            return &bridgeGroup->GetBridgeGroupState();
        } else {
            Y_ABORT();
        }
    };

    THashMap<TGroupId, std::tuple<TNodeId, TGroupId>> workingSyncersForTargetGroups;
    std::vector<std::tuple<TNodeId, TGroupId, TGroupId>> syncersToTerminate;

    for (const auto& [nodeId, targetGroupId, sourceGroupId] : SyncersNodeTargetSource) {
        const NKikimrBridge::TGroupState *state = getGroupStateByTargetGroup(targetGroupId);

        // we have a syncer for target group; check if it syncs correctly from really synced group
        bool sourceGroupCorrect = false;
        for (const auto& pile : state->GetPile()) {
            const auto refGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (sourceGroupId == refGroupId) {
                if (pile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                    sourceGroupCorrect = true;
                }
                break;
            }
        }
        if (!sourceGroupCorrect) {
            syncersToTerminate.emplace_back(nodeId, targetGroupId, sourceGroupId);
        } else if (const auto it = workingSyncersForTargetGroups.find(targetGroupId); it != workingSyncersForTargetGroups.end()) {
            // there is a syncer already running for this target group, we have to pick the one we need to terminate; prefer
            // one from disconnected node
            const auto& [existingNodeId, existingSourceGroupId] = it->second;
            const TNodeInfo *existingNode = FindNode(existingNodeId);
            Y_ABORT_UNLESS(existingNode);
            if (!existingNode->ConnectedServerId) { // the existing one is disconnected, drop it and replace with new
                syncersToTerminate.emplace_back(existingNodeId, targetGroupId, existingSourceGroupId);
                it->second = {nodeId, sourceGroupId};
            } else { // drop the new one
                syncersToTerminate.emplace_back(nodeId, targetGroupId, sourceGroupId);
            }
        } else {
            workingSyncersForTargetGroups[targetGroupId] = {nodeId, sourceGroupId};
        }
    }

    THashSet<TNodeId> nodesToUpdate;
    for (const auto& [nodeId, targetGroupId, sourceGroupId] : syncersToTerminate) {
        SyncersNodeTargetSource.erase(std::make_tuple(nodeId, targetGroupId, sourceGroupId));
        SyncersTargetNodeSource.erase(std::make_tuple(targetGroupId, nodeId, sourceGroupId));
        nodesToUpdate.insert(nodeId);
    }

    // now calculate the ones we need to create
    auto runNewSyncers = [&](auto& bridgeGroupInfo) {
        if (!bridgeGroupInfo || !bridgeGroupInfo->HasBridgeGroupState()) {
            return;
        }
        const auto& state = bridgeGroupInfo->GetBridgeGroupState();
        for (const auto& pile : state.GetPile()) {
            if (pile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                continue; // this group is synced
            }
            const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (TargetGroupsInCommit.contains(targetGroupId)) {
                continue;
            }
            const auto it = SyncersTargetNodeSource.lower_bound(std::make_tuple(targetGroupId, TNodeId(), TGroupId()));
            if (it != SyncersTargetNodeSource.end() && std::get<0>(*it) == targetGroupId) {
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR01, "StartRequiredSyncers: already running",
                    (TargetGroupId, targetGroupId), (NodeId, std::get<1>(*it)), (SourceGroupId, std::get<2>(*it)));
                continue; // syncer is already running
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
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR02, "StartRequiredSyncers: no nodes to start syncer at",
                    (TargetGroupId, targetGroupId), (SourceGroupId, *sourceGroupId));
                continue;
            }

            // pick random one
            const size_t index = RandomNumber(nodes.size());
            const TNodeId nodeId = nodes[index];

            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR03, "StartRequiredSyncers: starting syncer",
                (TargetGroupId, targetGroupId), (SourceGroupId, *sourceGroupId), (Nodes, nodes), (NodeId, nodeId),
                (GroupPile, pile));

            SyncersNodeTargetSource.emplace(nodeId, targetGroupId, *sourceGroupId);
            SyncersTargetNodeSource.emplace(targetGroupId, nodeId, *sourceGroupId);
            nodesToUpdate.insert(nodeId);
        }
    };
    for (const auto& [groupId, group] : StaticGroups) {
        runNewSyncers(group.Info->Group);
    }
    for (const auto& [groupId, group] : GroupMap) {
        runNewSyncers(group->BridgeGroupInfo);
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
    for (auto it = SyncersNodeTargetSource.lower_bound(std::make_tuple(nodeId, TGroupId(), TGroupId()));
            it != SyncersNodeTargetSource.end() && std::get<0>(*it) == nodeId; ++it) {
        const auto& [nodeId, targetGroupId, sourceGroupId] = *it;
        TGroupId bridgeProxyGroupId;
        ui32 generation = 0;
        bool staticGroup = false;
        if (const TGroupInfo *group = FindGroup(targetGroupId)) {
            Y_ABORT_UNLESS(group->BridgeProxyGroupId);
            bridgeProxyGroupId = *group->BridgeProxyGroupId;
            generation = group->Generation;
            staticGroup = false;
        } else if (const auto it = StaticGroups.find(targetGroupId); it != StaticGroups.end()) {
            const auto& group = it->second.Info->Group;
            Y_ABORT_UNLESS(group);
            Y_ABORT_UNLESS(group->HasBridgeProxyGroupId());
            bridgeProxyGroupId = TGroupId::FromProto(&group.value(), &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId);
            generation = group->GetGroupGeneration();
            staticGroup = true;
        } else {
            Y_ABORT();
        }
        auto *syncer = update->AddSyncers();
        bridgeProxyGroupId.CopyToProto(syncer, &std::decay_t<decltype(*syncer)>::SetBridgeProxyGroupId);
        syncer->SetBridgeProxyGroupGeneration(generation);
        sourceGroupId.CopyToProto(syncer, &std::decay_t<decltype(*syncer)>::SetSourceGroupId);
        targetGroupId.CopyToProto(syncer, &std::decay_t<decltype(*syncer)>::SetTargetGroupId);
        if (!staticGroup) {
            groupIdsToRead.insert({bridgeProxyGroupId.GetRawId(), sourceGroupId.GetRawId(), targetGroupId.GetRawId()});
        }
    }
    update->SetUpdateSyncers(true);
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerUpdateSyncerState::TPtr ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCBR04, "TEvControllerUpdateSyncerState", (Msg, ev->Get()->Record));
    TSet<ui32> groupIdsToRead;
    const TNodeId nodeId = ev->Sender.NodeId();
    ApplySyncerState(nodeId, ev->Get()->Record, groupIdsToRead);
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
                    TABLEH() { out << "Source group id"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "Node id"; }
                }
            }
            TABLEBODY() {
                for (auto& [targetGroupId, nodeId, sourceGroupId] : SyncersTargetNodeSource) {
                    TABLER() {
                        TGroupId bridgeProxyGroupId;
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
                        if (const TGroupInfo *group = FindGroup(targetGroupId)) {
                            if (group->BridgeProxyGroupId) {
                                bridgeProxyGroupId = *group->BridgeProxyGroupId;
                                if (const TGroupInfo *bridgeProxyGroup = FindGroup(bridgeProxyGroupId)) {
                                    scanPiles(bridgeProxyGroup->BridgeGroupInfo);
                                }
                            }
                        } else if (const auto it = StaticGroups.find(targetGroupId); it != StaticGroups.end()) {
                            if (const auto& group = it->second.Info->Group) {
                                bridgeProxyGroupId = TGroupId::FromProto(&group.value(),
                                    &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId);
                                scanPiles(group);
                            }
                        }
                        TABLED() { out << bridgeProxyGroupId; }
                        TABLED() { out << targetGroupId; }
                        TABLED() { out << sourceGroupId; }
                        TABLED() { out << state; }
                        TABLED() {
                            out << "<a href='/node/" << nodeId << "/actors/nodewarden#syncer-" << targetGroupId << "'>" << nodeId << "</a>";
                        }
                    }
                }
            }
        }
    }
}

} // NKikimr::NBsController
