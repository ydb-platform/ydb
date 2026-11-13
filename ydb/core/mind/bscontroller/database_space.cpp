#include "database_space.h"
#include "impl.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxUpdateDatabaseSpace : public TTransactionBase<TBlobStorageController> {
        const std::map<TBoxStoragePoolId, bool> Latches; // the ones changed, see PersistChangedDatabaseSpaceLatches

    public:
        TTxUpdateDatabaseSpace(TBlobStorageController *controller, std::map<TBoxStoragePoolId, bool> latches)
            : TTransactionBase(controller)
            , Latches(std::move(latches))
        {}

        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_DATABASE_SPACE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            Self->PersistChangedDatabaseSpaceLatches(db, Latches);
            return true;
        }

        void Complete(const TActorContext&) override {}
    };

    void TBlobStorageController::UpdateDatabaseSpaceOnPileChanges() {
        // Groups are recalculated right here, along with the BridgeInfo change, and only the latches are left to the
        // transaction: any group recalculated later on its own (e.g. by VDisk metrics) uses the new BridgeInfo, and it
        // must never be weighed against the groups still counted by the previous one, as hysteresis would keep the
        // pool state that never was.
        {
            TDatabaseSpaceTracker::TBatch batch(DatabaseSpace);
            for (const auto& [groupId, group] : GroupMap) {
                if (group->BridgeGroupInfo) {
                    UpdateDatabaseSpaceGroup(*group);
                }
            }
        }
        if (auto latches = PublishDatabaseSpaceChanges(); !latches.empty()) {
            Execute(new TTxUpdateDatabaseSpace(this, std::move(latches)));
        }
    }

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerSubscribeDatabaseSpace::TPtr ev) {
        const TNodeId nodeId = ev->Sender.NodeId(); // already validated to match the registered node
        const auto& record = ev->Get()->Record;
        YDB_LOG_DEBUG("TEvControllerSubscribeDatabaseSpace",
            {"marker", "BSCDS01"},
            {"nodeId", nodeId},
            {"record", record});

        std::vector<TDatabaseSpaceTracker::TScope> scopes;
        for (const auto& scope : record.GetUnsubscribe()) {
            scopes.push_back(TPathId::FromProto(scope));
        }
        DatabaseSpace.Unsubscribe(nodeId, scopes);

        scopes.clear();
        for (const auto& scope : record.GetSubscribe()) {
            scopes.push_back(TPathId::FromProto(scope));
        }
        DatabaseSpace.Subscribe(nodeId, scopes);
        for (const auto& scope : scopes) {
            SendDatabaseSpaceState(nodeId, scope);
        }
    }

    void TBlobStorageController::PersistDatabaseSpaceLatches(NIceDb::TNiceDb& db,
            const std::map<TBoxStoragePoolId, bool>& latches) {
        // the pools must exist (or be created in the same transaction); rows of deleted pools are removed along with
        // the pools (see CommitDatabaseSpaceUpdates)
        using T = Schema::DatabaseSpaceExhaustedPool;
        for (const auto& [poolId, exhausted] : latches) {
            const auto& [boxId, storagePoolId] = poolId;
            if (exhausted) {
                db.Table<T>().Key(boxId, storagePoolId).Update();
            } else {
                db.Table<T>().Key(boxId, storagePoolId).Delete();
            }
        }
    }

    void TBlobStorageController::PersistChangedDatabaseSpaceLatches(NIceDb::TNiceDb& db,
            const std::map<TBoxStoragePoolId, bool>& changed) {
        // The transaction persisting the latches changed outside of it may run after the ones queued before it, which
        // have changed the same latches further (thresholds, groups) and persisted them, or have deleted the pools along
        // with their rows. So the value taken when the latch changed may be stale by now, and the current one is
        // written: it follows from everything executed so far, and whatever changes it later persists it again.
        std::map<TBoxStoragePoolId, bool> latches;
        for (const auto& [poolId, exhausted] : changed) {
            if (!StoragePools.contains(poolId)) {
                continue;
            }
            if (const auto state = DatabaseSpace.GetPoolState(poolId)) {
                latches.emplace(poolId, state->Exhausted);
            }
        }
        PersistDatabaseSpaceLatches(db, latches);
    }

    std::map<TBoxStoragePoolId, bool> TBlobStorageController::PublishDatabaseSpaceChanges() {
        // pool colors are shown in system views
        for (const TBoxStoragePoolId& poolId : DatabaseSpace.TakeChangedPools()) {
            SysViewChangedStoragePools.insert(poolId);
        }

        for (const auto& scope : DatabaseSpace.TakeChangedScopes()) {
            if (const auto *nodes = DatabaseSpace.GetSubscribers(scope)) {
                for (const TNodeId nodeId : *nodes) {
                    SendDatabaseSpaceState(nodeId, scope);
                }
            }
        }

        // hysteresis latches must survive restarts, the caller persists them
        return DatabaseSpace.TakeChangedLatches();
    }

    void TBlobStorageController::CommitDatabaseSpaceChanges(NIceDb::TNiceDb& db) {
        PersistDatabaseSpaceLatches(db, PublishDatabaseSpaceChanges());
    }

    void TBlobStorageController::SendDatabaseSpaceState(TNodeId nodeId, TDatabaseSpaceTracker::TScope scope) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerDatabaseSpaceState>(scope, DatabaseSpace.IsExhausted(scope));
        YDB_LOG_DEBUG("SendDatabaseSpaceState",
            {"marker", "BSCDS02"},
            {"nodeId", nodeId},
            {"record", ev->Record});
        SendToWarden(nodeId, std::move(ev), 0);
    }

    void TBlobStorageController::UpdateDatabaseSpaceGroup(const TGroupInfo& group) {
        const TGroupInfo::TGroupFinder finder = [this](TGroupId groupId) {
            return FindGroup(groupId);
        };
        UpdateDatabaseSpaceGroup(group, finder, BridgeInfo.get());
    }

    namespace {

        // The color of VDisks taking writes of a group: the worst color of the ones that have reported, and whether all
        // of them have (until then the color is only known not to be better than that).
        struct TVDisksSpaceColor {
            std::optional<TDatabaseSpaceTracker::ESpaceColor> Color;
            bool Complete = true;

            void Add(const TBlobStorageController::TGroupInfo& group) {
                if (const auto color = StatusFlagToValidSpaceColor(group.StatusFlags.Raw)) {
                    Color = Color ? Max(*Color, *color) : *color;
                }
                for (const TBlobStorageController::TVSlotInfo *slot : group.VDisksInGroup) {
                    if (!StatusFlagToValidSpaceColor(slot->Metrics.GetStatusFlags())) {
                        Complete = false;
                        break;
                    }
                }
            }
        };

    } // anonymous

    void TBlobStorageController::UpdateDatabaseSpaceGroup(const TGroupInfo& group,
            const TGroupInfo::TGroupFinder& finder, const TBridgeInfo *bridgeInfo) {
        if (group.BridgeProxyGroupId && !group.IsInlinePile()) {
            DatabaseSpace.RemoveGroup(group.ID);
            if (const TGroupInfo *proxy = finder(*group.BridgeProxyGroupId)) {
                UpdateDatabaseSpaceGroup(*proxy, finder, bridgeInfo);
            }
            return;
        }
        if (!group.WritesGoToVDisks()) {
            DatabaseSpace.RemoveGroup(group.ID);
            return;
        }
        TVDisksSpaceColor res;
        if (group.BridgeGroupInfo) {
            if (!bridgeInfo) {
                DatabaseSpace.SetGroup(group.ID, group.StoragePoolId, std::nullopt, false);
                return;
            }
            // the parts are counted by the rules bridge proxy follows when it sends TEvPut: the pile has to take part in
            // data quorum, and the part has to be at the stage taking data (the one being synced or dropped is not)
            NBridge::ForEachGroupPile(group.BridgeGroupInfo->GetBridgeGroupState(), [&](TBridgePileId pileId,
                    const NKikimrBridge::TGroupState::TPile& pile) {
                const TBridgeInfo::TPile *pileInfo = bridgeInfo->FindPile(pileId);
                if (!pileInfo || !NBridge::PileStateTraits(pileInfo->State).RequiresDataQuorum ||
                        !NBridge::GroupPartTakesPuts(pile.GetStage())) {
                    return;
                }
                const TGroupId partId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                const TGroupInfo *part = partId == group.ID ? &group : finder(partId);
                if (!part || part->VDisksInGroup.empty()) {
                    res.Complete = false;
                    return;
                }
                res.Add(*part);
            });
        } else if (group.VDisksInGroup.empty()) {
            DatabaseSpace.RemoveGroup(group.ID);
            return;
        } else {
            res.Add(group);
        }
        DatabaseSpace.SetGroup(group.ID, group.StoragePoolId, res.Color, res.Complete);
    }

    void TBlobStorageController::UpdateDatabaseSpacePool(const TBoxStoragePoolId& poolId, const TStoragePoolInfo& pool) {
        std::optional<TDatabaseSpaceTracker::TScope> scope;
        if (pool.SchemeshardId && pool.PathItemId) {
            scope.emplace(*pool.SchemeshardId, *pool.PathItemId);
        }
        DatabaseSpace.SetPool(poolId, scope);
    }

    bool TDatabaseSpaceTracker::IsValidThresholds(ESpaceColor block, ESpaceColor unblock) {
        return unblock == NKikimrBlobStorage::TPDiskSpaceColor::GREEN || unblock <= block;
    }

    void TDatabaseSpaceTracker::SetThresholds(ESpaceColor block, ESpaceColor unblock) {
        Y_DEBUG_ABORT_UNLESS(IsValidThresholds(block, unblock));
        BlockColor = block;
        UnblockColor = unblock;
        for (const auto& [poolId, pool] : Pools) {
            Recalculate(poolId);
            ChangedPools.insert(poolId);
        }
    }

    TDatabaseSpaceTracker::ESpaceColor TDatabaseSpaceTracker::GetEffectiveUnblockColor() const {
        return UnblockColor != NKikimrBlobStorage::TPDiskSpaceColor::GREEN ? UnblockColor : BlockColor;
    }

    void TDatabaseSpaceTracker::SetPool(TBoxStoragePoolId poolId, std::optional<TScope> scope) {
        TPool& pool = Pools[poolId];
        ChangedPools.insert(poolId);
        if (pool.Scope != scope) {
            if (pool.Scope) {
                MarkChanged(pool);
                if (const auto it = ScopePools.find(*pool.Scope); it != ScopePools.end()) {
                    it->second.erase(poolId);
                    if (it->second.empty()) {
                        ScopePools.erase(it);
                    }
                }
            }
            pool.Scope = scope;
            if (pool.Scope) {
                ScopePools[*pool.Scope].insert(poolId);
                MarkChanged(pool);
            }
        }
    }

    void TDatabaseSpaceTracker::RemovePool(TBoxStoragePoolId poolId) {
        const auto it = Pools.find(poolId);
        if (it == Pools.end()) {
            return;
        }
        SetPool(poolId, std::nullopt); // detach from the scope
        EraseNodesIf(Groups, [&](const auto& item) { return item.second.PoolId == poolId; });
        ChangedLatches.erase(poolId); // the persisted latch is deleted along with the pool by the caller
        DirtyPools.erase(poolId);
        Pools.erase(it);
    }

    void TDatabaseSpaceTracker::RestorePoolExhausted(TBoxStoragePoolId poolId, bool exhausted) {
        Y_DEBUG_ABORT_UNLESS(BatchDepth); // the latch must be evaluated only after the pool's groups are counted
        if (const auto it = Pools.find(poolId); it != Pools.end()) {
            it->second.Exhausted = exhausted;
            MarkChanged(it->second);
            Recalculate(poolId);
        }
    }

    void TDatabaseSpaceTracker::SetGroup(TGroupId groupId, TBoxStoragePoolId poolId, std::optional<ESpaceColor> color,
            bool complete) {
        const TGroup group{poolId, color, complete && color};
        // pools are recalculated only after all the counters are updated: a group changing its color must not be
        // seen as temporarily missing from its pool, or else hysteresis would keep the wrong state
        std::optional<TBoxStoragePoolId> prevPoolId;
        if (const auto it = Groups.find(groupId); it != Groups.end()) {
            if (it->second.PoolId == group.PoolId && it->second.Color == group.Color && it->second.Complete == group.Complete) {
                return; // nothing has changed
            }
            prevPoolId = it->second.PoolId;
            UncountGroup(it->second);
            Groups.erase(it);
        }
        Groups.emplace(groupId, group);
        CountGroup(group);
        Recalculate(poolId);
        if (prevPoolId && *prevPoolId != poolId) {
            Recalculate(*prevPoolId);
        }
    }

    void TDatabaseSpaceTracker::RemoveGroup(TGroupId groupId) {
        if (const auto it = Groups.find(groupId); it != Groups.end()) {
            const TBoxStoragePoolId poolId = it->second.PoolId;
            UncountGroup(it->second);
            Groups.erase(it);
            Recalculate(poolId);
        }
    }

    void TDatabaseSpaceTracker::ResetState() {
        for (const auto& [scope, pools] : ScopePools) {
            ChangedScopes.insert(scope);
        }
        Pools.clear();
        Groups.clear();
        ScopePools.clear();
        DirtyPools.clear();
        ChangedLatches.clear();
    }

    void TDatabaseSpaceTracker::Recalculate(TBoxStoragePoolId poolId) {
        if (BatchDepth) {
            DirtyPools.insert(poolId);
        } else if (const auto it = Pools.find(poolId); it != Pools.end()) {
            RecalculatePool(poolId, it->second);
        }
    }

    void TDatabaseSpaceTracker::RecalculateDirtyPools() {
        for (const TBoxStoragePoolId& poolId : std::exchange(DirtyPools, {})) {
            if (const auto it = Pools.find(poolId); it != Pools.end()) {
                RecalculatePool(poolId, it->second);
            }
        }
    }

    bool TDatabaseSpaceTracker::IsExhausted(TScope scope) const {
        if (const auto it = ScopePools.find(scope); it != ScopePools.end()) {
            for (const TBoxStoragePoolId& poolId : it->second) {
                if (Pools.at(poolId).Exhausted) {
                    return true;
                }
            }
        }
        return false;
    }

    std::optional<TDatabaseSpaceTracker::TPoolState> TDatabaseSpaceTracker::GetPoolState(TBoxStoragePoolId poolId) const {
        const auto it = Pools.find(poolId);
        if (it == Pools.end()) {
            return std::nullopt;
        }
        const TPool& pool = it->second;
        TPoolState res;
        if (!pool.Colors.empty()) {
            res.BestColor = pool.Colors.begin()->first;
            res.WorstColor = pool.Colors.rbegin()->first;
        }
        res.NumGroups = pool.NumGroups;
        res.NumUnknownGroups = pool.NumUnknownGroups;
        res.NumIncompleteGroups = pool.NumIncompleteGroups;
        res.Exhausted = pool.Exhausted;
        return res;
    }

    std::vector<TDatabaseSpaceTracker::TScope> TDatabaseSpaceTracker::TakeChangedScopes() {
        std::vector<TScope> res;
        for (const TScope& scope : std::exchange(ChangedScopes, {})) {
            if (Subscribers.contains(scope)) {
                res.push_back(scope);
            }
        }
        return res;
    }

    void TDatabaseSpaceTracker::Subscribe(TNodeId nodeId, const std::vector<TScope>& scopes) {
        for (const TScope& scope : scopes) {
            Subscribers[scope].insert(nodeId);
            NodeSubscriptions[nodeId].insert(scope);
        }
    }

    void TDatabaseSpaceTracker::Unsubscribe(TNodeId nodeId, const std::vector<TScope>& scopes) {
        const auto nodeIt = NodeSubscriptions.find(nodeId);
        if (nodeIt == NodeSubscriptions.end()) {
            return;
        }
        for (const TScope& scope : scopes) {
            nodeIt->second.erase(scope);
            if (const auto it = Subscribers.find(scope); it != Subscribers.end()) {
                it->second.erase(nodeId);
                if (it->second.empty()) {
                    Subscribers.erase(it);
                }
            }
        }
        if (nodeIt->second.empty()) {
            NodeSubscriptions.erase(nodeIt);
        }
    }

    void TDatabaseSpaceTracker::UnsubscribeNode(TNodeId nodeId) {
        if (const auto it = NodeSubscriptions.find(nodeId); it != NodeSubscriptions.end()) {
            const std::vector<TScope> scopes(it->second.begin(), it->second.end());
            Unsubscribe(nodeId, scopes);
        }
    }

    const std::set<TNodeId> *TDatabaseSpaceTracker::GetSubscribers(TScope scope) const {
        const auto it = Subscribers.find(scope);
        return it != Subscribers.end() ? &it->second : nullptr;
    }

    void TDatabaseSpaceTracker::CountGroup(const TGroup& group) {
        TPool& pool = Pools[group.PoolId];
        if (group.Color) {
            ++pool.Colors[*group.Color];
        } else {
            ++pool.NumUnknownGroups;
        }
        if (group.Complete) {
            ++pool.CompleteColors[*group.Color];
        } else {
            ++pool.NumIncompleteGroups;
        }
        ++pool.NumGroups;
        ChangedPools.insert(group.PoolId);
    }

    void TDatabaseSpaceTracker::UncountGroup(const TGroup& group) {
        const auto poolIt = Pools.find(group.PoolId);
        Y_DEBUG_ABORT_UNLESS(poolIt != Pools.end());
        if (poolIt == Pools.end()) {
            return;
        }
        TPool& pool = poolIt->second;
        auto uncountColor = [](std::map<ESpaceColor, ui32>& colors, ESpaceColor color) {
            const auto it = colors.find(color);
            Y_DEBUG_ABORT_UNLESS(it != colors.end() && it->second);
            if (it != colors.end() && !--it->second) {
                colors.erase(it);
            }
        };
        if (group.Color) {
            uncountColor(pool.Colors, *group.Color);
        } else {
            Y_DEBUG_ABORT_UNLESS(pool.NumUnknownGroups);
            --pool.NumUnknownGroups;
        }
        if (group.Complete) {
            uncountColor(pool.CompleteColors, *group.Color);
        } else {
            Y_DEBUG_ABORT_UNLESS(pool.NumIncompleteGroups);
            --pool.NumIncompleteGroups;
        }
        --pool.NumGroups;
        ChangedPools.insert(group.PoolId);
    }

    void TDatabaseSpaceTracker::RecalculatePool(TBoxStoragePoolId poolId, TPool& pool) {
        bool exhausted = pool.Exhausted;
        if (BlockColor == NKikimrBlobStorage::TPDiskSpaceColor::GREEN || !pool.NumGroups) {
            exhausted = false; // disabled or there are no groups in the pool
        } else {
            if (!pool.CompleteColors.empty() && pool.CompleteColors.begin()->first < GetEffectiveUnblockColor()) {
                exhausted = false; // some group is known to be good enough
            } else if (!pool.NumUnknownGroups && pool.Colors.begin()->first >= BlockColor) {
                exhausted = true; // every group has reached the block color (known even from a part of its VDisks)
            }
            // otherwise the state is kept: the best color is between the unblock and block ones, or not every VDisk has
            // reported its color yet (e.g. metrics are missing after a group has been reconfigured and BS_CONTROLLER
            // restarted), so a group better than the unblock color may yet turn out to be worse
        }
        if (exhausted != pool.Exhausted) {
            pool.Exhausted = exhausted;
            MarkChanged(pool);
            ChangedLatches[poolId] = exhausted;
            ChangedPools.insert(poolId);
        }
    }

    void TDatabaseSpaceTracker::MarkChanged(const TPool& pool) {
        if (pool.Scope) {
            ChangedScopes.insert(*pool.Scope);
        }
    }

} // NKikimr::NBsController
