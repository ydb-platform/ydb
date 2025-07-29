#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::PrepareScatterTask(ui64 cookie, TScatterTask& task, const TEvScatter::TManageSyncers& request) {
        // issue query to node warden and wait until its completion
        std::vector<TEvNodeWardenManageSyncers::TSyncer> runSyncers;
        for (const auto& item : request.GetRunSyncers()) {
            runSyncers.push_back({
                .NodeId = item.GetNodeId(),
                .GroupId = TGroupId::FromProto(&item, &NKikimrBlobStorage::TStorageSyncerInfo::GetGroupId),
                .TargetBridgePileId = TBridgePileId::FromProto(&item, &NKikimrBlobStorage::TStorageSyncerInfo::GetTargetBridgePileId),
            });
        }
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenManageSyncers(std::move(runSyncers)), 0, cookie);
        ++task.AsyncOperationsPending;
    }

    void TDistributedConfigKeeper::Handle(TEvNodeWardenManageSyncersResult::TPtr ev) {
        if (auto it = ScatterTasks.find(ev->Cookie); it != ScatterTasks.end()) {
            TScatterTask& task = it->second;
            auto *response = task.Response.MutableManageSyncers();
            auto *node = response->AddNodes();
            node->SetNodeId(SelfId().NodeId());
            for (const auto& workingSyncer : ev->Get()->WorkingSyncers) {
                auto *item = node->AddSyncers();
                workingSyncer.GroupId.CopyToProto(item, &NKikimrBlobStorage::TStorageSyncerInfo::SetGroupId);
                workingSyncer.TargetBridgePileId.CopyToProto(item, &NKikimrBlobStorage::TStorageSyncerInfo::SetTargetBridgePileId);
            }
        }
        FinishAsyncOperation(ev->Cookie);
    }

    void TDistributedConfigKeeper::Perform(TEvGather::TManageSyncers *response, const TEvScatter::TManageSyncers& /*request*/, TScatterTask& task) {
        THashMap<ui32, TEvGather::TManageSyncers::TNode*> nodeMap;
        for (size_t i = 0; i < response->NodesSize(); ++i) {
            auto *node = response->MutableNodes(i);
            nodeMap.emplace(node->GetNodeId(), node);
        }
        for (const auto& reply : task.CollectedResponses) {
            for (const auto& node : reply.GetManageSyncers().GetNodes()) {
                if (const auto it = nodeMap.find(node.GetNodeId()); it != nodeMap.end()) {
                    it->second->MergeFrom(node);
                } else {
                    auto *newNode = response->AddNodes();
                    newNode->CopyFrom(node);
                    nodeMap.emplace(newNode->GetNodeId(), newNode);
                }
            }
        }
    }

    void TDistributedConfigKeeper::ProcessManageSyncers(TEvGather::TManageSyncers *res) {
        // actualize WorkingSyncers with received data
        for (const auto& node : res->GetNodes()) {
            const ui32 nodeId = node.GetNodeId();

            // trim currently known working syncers for this node (as we received a bit freshier info)
            const auto min = std::make_tuple(nodeId, TGroupId::Min(), TBridgePileId::Min());
            const auto begin = WorkingSyncersByNode.lower_bound(min);
            const auto max = std::make_tuple(nodeId, TGroupId::Max(), TBridgePileId::Max());
            const auto end = WorkingSyncersByNode.upper_bound(max);
            for (auto it = begin; it != end; ++it) {
                const auto& [nodeId, groupId, targetBridgePileId] = *it;
                const size_t num = WorkingSyncers.erase(std::make_tuple(groupId, targetBridgePileId, nodeId));
                Y_ABORT_UNLESS(num == 1);
            }
            WorkingSyncersByNode.erase(begin, end);

            // insert newly received syncers in the set
            for (const auto& item : node.GetSyncers()) {
                using T = std::decay_t<decltype(item)>;
                const auto groupId = TGroupId::FromProto(&item, &T::GetGroupId);
                const auto targetBridgePileId = TBridgePileId::FromProto(&item, &T::GetTargetBridgePileId);
                const bool ins1 = WorkingSyncersByNode.emplace(nodeId, groupId, targetBridgePileId).second;
                const bool ins2 = WorkingSyncers.emplace(groupId, targetBridgePileId, nodeId).second;
                Y_ABORT_UNLESS(ins1 == ins2);
            }
        }

        // apply changes
        RearrangeSyncing();
    }

    void TDistributedConfigKeeper::RearrangeSyncing() {
        // run new syncers, stop unneeded ones
        TEvScatter task;
        TEvScatter::TManageSyncers *manage = nullptr;

        auto getManage = [&] {
            if (!manage) {
                manage = task.MutableManageSyncers();
                task.SetTaskId(RandomNumber<ui64>());
            }
            return manage;
        };

        for (auto it = WorkingSyncers.begin(); it != WorkingSyncers.end(); ) {
            const auto& [groupId, targetBridgePileId, nodeId] = *it;

            // find an end to this sequence
            size_t numItems = 0;
            auto jt = it;
            while (jt != WorkingSyncers.end() && std::get<0>(*jt) == groupId && std::get<1>(*jt) == targetBridgePileId) {
                ++jt, ++numItems;
            }

            // we have to terminate excessive syncer(s) here
            if (numItems > 1) {
                auto *entry = getManage()->AddRunSyncers(); // explicitly let only single one remaining
                const auto& [groupId, targetBridgePileId, nodeId] = *it;
                entry->SetNodeId(nodeId);
                groupId.CopyToProto(entry, &NKikimrBlobStorage::TStorageSyncerInfo::SetGroupId);
                targetBridgePileId.CopyToProto(entry, &NKikimrBlobStorage::TStorageSyncerInfo::SetTargetBridgePileId);
            }

            // advance to next pair
            it = jt;
        }

        std::vector<ui32> nodes;
        auto prepareNodes = [&] {
            if (nodes.empty()) {
                // build list of all nodes expected to be working
                nodes.push_back(SelfNode.NodeId());
                for (const auto& [nodeId, info] : AllBoundNodes) {
                    nodes.push_back(nodeId.NodeId());
                }
                std::ranges::sort(nodes);
            }
        };

        Y_ABORT_UNLESS(StorageConfig);
        const auto& history = StorageConfig->GetClusterStateHistory();
        for (const auto& item : history.GetPileSyncState()) {
            const auto bridgePileId = TBridgePileId::FromProto(&item,
                &NKikimrBridge::TClusterStateHistory::TPileSyncState::GetBridgePileId);

            for (const auto& groupIdNum : item.GetUnsyncedGroupIds()) {
                const auto groupId = TGroupId::FromValue(groupIdNum);

                const auto key = std::make_tuple(groupId, bridgePileId, 0);
                const auto maxKey = std::make_tuple(groupId, bridgePileId, Max<ui32>());
                const auto it = WorkingSyncers.lower_bound(key);
                if (it != WorkingSyncers.end() && *it <= maxKey) {
                    continue; // syncer already running
                }

                auto *entry = getManage()->AddRunSyncers();
                const size_t hash = MultiHash(groupId, bridgePileId);
                prepareNodes();
                entry->SetNodeId(nodes[hash % nodes.size()]);
                groupId.CopyToProto(entry, &NKikimrBlobStorage::TStorageSyncerInfo::SetGroupId);
                bridgePileId.CopyToProto(entry, &NKikimrBlobStorage::TStorageSyncerInfo::SetTargetBridgePileId);
            }
        }

        if (manage) {
            IssueScatterTask(TActorId(), std::move(task));
        } else if (SyncerArrangeInFlight) {
            SyncerArrangeInFlight = false;
            if (std::exchange(SyncerArrangePending, false)) {
                IssueQuerySyncers();
            }
        }
    }

    void TDistributedConfigKeeper::OnSyncerUnboundNode(ui32 nodeId) {
        bool changes = false;

        const auto min = std::make_tuple(nodeId, TGroupId::Min(), TBridgePileId::Min());
        for (auto it = WorkingSyncersByNode.lower_bound(min); it != WorkingSyncersByNode.end() &&
                std::get<0>(*it) == nodeId; it = WorkingSyncersByNode.erase(it)) {
            const auto& [nodeId, groupId, targetBridgePileId] = *it;
            const size_t num = WorkingSyncers.erase(std::make_tuple(groupId, targetBridgePileId, nodeId));
            Y_ABORT_UNLESS(num == 1);
            changes = true;
        }

        if (changes) {
            RearrangeSyncing();
        }
    }

    void TDistributedConfigKeeper::IssueQuerySyncers() {
        if (!Cfg->BridgeConfig || !Scepter) {
            return;
        }
        if (!SyncerArrangeInFlight) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC82, "Starting syncer collection", (Scepter, Scepter->Id), (RootState, RootState));
            TEvScatter task;
            task.SetTaskId(RandomNumber<ui64>());
            task.MutableManageSyncers();
            IssueScatterTask(TActorId(), std::move(task));
            SyncerArrangeInFlight = true;
        } else {
            SyncerArrangePending = true;
        }
    }

    bool TDistributedConfigKeeper::UpdateBridgeConfig(NKikimrBlobStorage::TStorageConfig *config,
            const THashMap<TBridgePileId, NKikimrBlobStorage::TStorageConfig*>& persistedConfigForUnsyncedPile) {
        bool changes = false;
        const ui64 configGeneration = config->GetGeneration();

        if (config->HasClusterStateHistory()) {
            Y_DEBUG_ABORT_UNLESS(config->HasClusterState());
            const auto& clusterState = config->GetClusterState();

            auto *history = config->MutableClusterStateHistory();
            auto *entries = history->MutableUnsyncedEntries();
            for (int i = 0; i < entries->size(); ++i) {
                auto *piles = entries->Mutable(i)->MutableUnsyncedPiles();
                for (int j = 0; j < piles->size(); ++j) {
                    auto pileId = piles->at(j);
                    if (pileId < clusterState.PerPileStateSize() &&
                            clusterState.GetPerPileState(pileId) != NKikimrBridge::TClusterState::DISCONNECTED) {
                        // we assume this pile is not disconnected and thus being the part of the storage config quorum,
                        // so it has this configuration persisted
                        piles->SwapElements(j--, piles->size() - 1);
                        piles->RemoveLast();
                        changes = true;
                    }
                }
                if (piles->empty() && i != entries->size() - 1) {
                    // drop fully synced entry (but keeping the last one)
                    entries->DeleteSubrange(i--, 1);
                    changes = true;
                }
            }

            for (size_t i = 0; i < history->PileSyncStateSize(); ++i) {
                if (auto *pss = history->MutablePileSyncState(i); pss->GetUnsyncedStorageConfig()) {
                    const auto bridgePileId = TBridgePileId::FromProto(pss, &std::decay_t<decltype(*pss)>::GetBridgePileId);
                    if (const auto it = persistedConfigForUnsyncedPile.find(bridgePileId); it != persistedConfigForUnsyncedPile.end()) {
                        NKikimrBlobStorage::TStorageConfig& peerConfig = *it->second;
                        pss->ClearUnsyncedStorageConfig();
                        config->SetGeneration(Max(configGeneration, peerConfig.GetGeneration()) + 1);
                        changes = true;
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
        state->SetGeneration(1);
        auto *piles = state->MutablePerPileState();
        for (size_t i = 0; i < cfg.BridgeConfig->PilesSize(); ++i) {
            piles->Add(NKikimrBridge::TClusterState::SYNCHRONIZED);
        }

        auto *history = config->MutableClusterStateHistory();
        auto *entry = history->AddUnsyncedEntries();
        entry->MutableClusterState()->CopyFrom(*state);
        for (int i = 0; i < piles->size(); ++i) {
            entry->AddUnsyncedPiles(i);
        }
    }

    bool TDistributedConfigKeeper::CheckBridgePeerRevPush(const NKikimrBlobStorage::TStorageConfig& peerConfig,
            ui32 senderNodeId) {
        if (!BridgeInfo) { // we aren't in bridge mode
            return true;
        }

        Y_ABORT_UNLESS(StorageConfig);

        const auto *peerPile = BridgeInfo->GetPileForNode(senderNodeId);
        if (!peerPile) {
            AbortBinding(TString(TStringBuilder() << "can't determine peer pile NodeId# " << senderNodeId).data());
            return false;
        }

        if (peerPile == BridgeInfo->SelfNodePile) { // accept all configs from the same pile
            return true;
        }

        // TODO(alexvru): consider peerConfig's picture of cluster

        if (peerPile->State != NKikimrBridge::TClusterState::SYNCHRONIZED) { // unexpected peer state
            AbortBinding("bound peer is not in SYNCHRONIZED state");
            return false;
        }

        if (BridgeInfo->SelfNodePile->State != NKikimrBridge::TClusterState::NOT_SYNCHRONIZED) { // we are not waiting for sync, accept configs
            return true;
        }

        if (!peerConfig.HasClusterStateHistory()) {
            AbortBinding("peer's config doesn't have ClusterStateHistory");
            return false;
        }
        for (const auto& item : peerConfig.GetClusterStateHistory().GetPileSyncState()) {
            if (TBridgePileId::FromProto(&item, &std::decay_t<decltype(item)>::GetBridgePileId) != BridgeInfo->SelfNodePile->BridgePileId) {
                continue; // some other pile
            } else if (item.GetUnsyncedStorageConfig()) {
                return false; // we can't let this config in as it is not merged with the primary pile yet
            }
        }

        return true; // validation successful
    }

} // NKikimr::NStorage
