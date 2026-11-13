#include "impl.h"
#include "bridge_storage.h"

#include <google/protobuf/timestamp.pb.h>

namespace {

    void SetTimestamp(TInstant instant, google::protobuf::Timestamp* to) {
        to->set_seconds(instant.Seconds());
        to->set_nanos(instant.MicroSecondsOfSecond() * 1000);
    }

} // namespace

namespace NKikimr::NBsController::NBridgeStorage {

    TCopyObservation ObserveCopy(const TCopyContext& context) {
        TCopyObservation result;
        const bool required = context.BridgeMode && context.PileState != NKikimrBridge::TClusterState::INACTIVE;
        if (context.Group.State) {
            result.Part = NBridge::GetGroupPile(*context.Group.State, context.PileId);
        }
        const bool authority = context.Group.AuthorityPileId == context.PileId;
        result.CurrentGeneration = authority || !context.PileSyncState || (result.Part && result.Part->GetBecameUnsyncedGeneration() >= context.PileSyncState->GetBecameUnsyncedGeneration());
        if (!required) {
            result.State = NKikimrBridge::NOT_REQUIRED;
        } else if (!context.Group.State) {
            result.State = NKikimrBridge::UNKNOWN;
        } else if (!result.Part || !result.CurrentGeneration) {
            result.State = NKikimrBridge::PENDING;
        } else if (!authority && (context.PileState == NKikimrBridge::TClusterState::DISCONNECTED ||
                                  context.PileState == NKikimrBridge::TClusterState::SUSPENDED)) {
            result.State = NKikimrBridge::UNKNOWN;
        } else if (result.Part->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
            result.State = NKikimrBridge::SYNCED;
        } else {
            result.State = context.SyncerRunning ? NKikimrBridge::SYNCING : NKikimrBridge::PENDING;
        }
        return result;
    }

    void TCounts::Count(ECopyState state) {
        if (state == NKikimrBridge::NOT_REQUIRED) {
            return;
        }
        ++Total;
        switch (state) {
            case NKikimrBridge::SYNCED:
                ++Synced;
                break;
            case NKikimrBridge::SYNCING:
                ++Running;
                break;
            case NKikimrBridge::PENDING:
                ++Pending;
                break;
            default:
                ++Unknown;
                break;
        }
    }

    void TCounts::CountDatabase(ECopyState state) {
        if (state == NKikimrBridge::NOT_REQUIRED) {
            ++Total;
            ++NotRequired;
        } else {
            Count(state);
        }
    }

    ECopyState TCounts::GetStatus() const {
        return GetBridgeSynchronizationStatus(Total - NotRequired, Synced, Running, Unknown);
    }

    void TCounts::ToProto(NKikimrBridge::TGroupSyncCounts* to) const {
        to->set_required(Total);
        to->set_synced(Synced);
        to->set_syncing(Running);
        to->set_pending(Pending);
        to->set_unknown(Unknown);
    }

    void TCounts::ToProto(NKikimrBridge::TDatabaseSyncCounts* to) const {
        to->set_total(Total);
        to->set_synced(Synced);
        to->set_syncing(Running);
        to->set_pending(Pending);
        to->set_unknown(Unknown);
        to->set_not_required(NotRequired);
    }

    bool TReportBudget::Visit() {
        if (Visited == TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxVisitedCandidates) {
            return false;
        }
        ++Visited;
        return true;
    }

    bool TReportBudget::Fits(ui64 bytes) const {
        return bytes <= TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxResultBytes - Bytes;
    }

    bool TReportBudget::Account(const google::protobuf::MessageLite& row) {
        const ui64 bytes = row.ByteSizeLong() + 6; // conservative tag and length prefix
        if (!Fits(bytes)) {
            return false;
        }
        Bytes += bytes;
        return true;
    }

    namespace {

        void CopyError(const TCopyObservation& copy, NKikimrBridge::TSynchronizationError* to) {
            to->set_message(*copy.LastError);
            if (copy.LastErrorTimestamp) {
                SetTimestamp(copy.LastErrorTimestamp, to->mutable_occurred_at());
            }
        }
    } // namespace

    bool AppendGroup(const TGroupRow& input, TReportBudget& budget, NKikimrBridge::TGroupSynchronizationResult& result) {
        const ui64 errorBytes = input.Copy.LastError ? input.Copy.LastError->size() : 0;
        const ui64 pileBytes = input.PileName ? input.PileName->size() : 0;
        const ui64 poolBytes = input.Pool ? input.Pool->name().size() : 0;
        if (!budget.Fits(errorBytes) || !budget.Fits(pileBytes) || !budget.Fits(poolBytes) ||
            !budget.Fits(errorBytes + pileBytes + poolBytes + 128)) {
            return false; // reject before copying an arbitrarily long upstream error
        }
        NKikimrBridge::TGroupSynchronization row;
        row.set_group_id(input.GroupId);
        if (input.PhysicalGroupId) {
            row.set_physical_group_id(*input.PhysicalGroupId);
        }
        if (input.Pool) {
            row.mutable_storage_pool()->CopyFrom(*input.Pool);
        }
        if (input.PileName) {
            row.set_pile_name(input.PileName->data(), input.PileName->size());
        }
        row.set_sync_status(input.Copy.State);
        if (input.Copy.Part) {
            row.set_sync_phase(NKikimrBridge::TGroupState::EStage_Name(input.Copy.Part->GetStage()));
        }
        if (input.Copy.LastError && (!input.Copy.LastError->empty() || input.Copy.LastErrorTimestamp)) {
            CopyError(input.Copy, row.mutable_last_sync_error());
        }
        if (!budget.Account(row)) {
            return false;
        }
        result.add_groups()->Swap(&row);
        return true;
    }

} // namespace NKikimr::NBsController::NBridgeStorage

namespace NKikimr::NBsController {
    namespace {

        constexpr TStringBuf LimitError = "Storage diagnostic result or scan is too large; narrow database/pool/pile/status filter";

        void CopyPool(TBoxStoragePoolId id, const TString& name, NKikimrBridge::TStoragePool* to) {
            to->mutable_id()->set_box_id(std::get<0>(id));
            to->mutable_id()->set_pool_id(std::get<1>(id));
            to->set_name(name);
        }

        struct TCopySummary {
            NBridgeStorage::TCounts Counts;
            NBridgeStorage::TCopyObservation LastError;

            void Add(const NBridgeStorage::TCopyObservation& copy) {
                Counts.Count(copy.State);
                if (LastError.LastErrorTimestamp < copy.LastErrorTimestamp) {
                    LastError = copy;
                }
            }
        };

        struct TOwnerSummary {
            NKikimrBridge::EStorageMode Mode = NKikimrBridge::STORAGE_MODE_UNSPECIFIED;
            TVector<TBoxStoragePoolId> Pools;
            ui64 PoolBytes = 0;
            std::map<TBridgePileId, TCopySummary> Copies;
        };

    } // namespace

    NBridgeStorage::TCopyObservation TBlobStorageController::ObserveBridgeStorageCopy(
        const NBridgeStorage::TGroupContext& group, const TBridgeInfo::TPile& pile,
        const NKikimrBridge::TClusterStateDetails::TPileSyncState* syncState, bool bridged) const {
        NBridgeStorage::TCopyContext context{
            .Group = group,
            .PileId = pile.BridgePileId,
            .PileState = pile.State,
            .PileSyncState = syncState,
            .BridgeMode = bridged,
        };
        if (const auto* part = group.State ? NBridge::GetGroupPile(*group.State, pile.BridgePileId) : nullptr) {
            const auto partId = TGroupId::FromProto(part, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (const auto it = TargetGroupToSyncerState.find(partId); it != TargetGroupToSyncerState.end()) {
                context.SyncerRunning = !it->second.NodeIds.empty();
            }
        }
        auto copy = NBridgeStorage::ObserveCopy(context);
        if (copy.Part && copy.CurrentGeneration && copy.State != NKikimrBridge::NOT_REQUIRED) {
            const auto partId = TGroupId::FromProto(copy.Part, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (const auto it = BridgeSyncState.find(partId); it != BridgeSyncState.end() && it->second.Stage == copy.Part->GetStage()) {
                copy.LastError = &it->second.LastError;
                copy.LastErrorTimestamp = it->second.LastErrorTimestamp;
            }
        }
        return copy;
    }

    NKikimrBridge::ESynchronizationStatus TBlobStorageController::GetBridgeSyncStatus(TGroupId groupId) {
        const NKikimrBridge::TGroupState* state = nullptr;
        TBridgePileId pileId;
        if (const auto* group = FindGroup(groupId)) {
            const auto pool = StoragePools.find(group->StoragePoolId);
            if (pool == StoragePools.end()) {
                return NKikimrBridge::UNKNOWN;
            }
            if (!pool->second.BridgeMode) {
                return NKikimrBridge::NOT_REQUIRED;
            }
            if (group->BridgePileId) {
                pileId = group->BridgePileId;
                const auto* proxy = group->BridgeProxyGroupId ? FindGroup(*group->BridgeProxyGroupId) : nullptr;
                if (proxy && proxy->BridgeGroupInfo) {
                    state = &proxy->BridgeGroupInfo->GetBridgeGroupState();
                }
            } else if (group->BridgeGroupInfo) {
                state = &group->BridgeGroupInfo->GetBridgeGroupState();
            }
        } else if (const auto it = StaticGroups.find(groupId); it != StaticGroups.end() &&
                it->second.Info && it->second.Info->Group) {
            const auto& group = *it->second.Info->Group;
            if (group.HasBridgePileId()) {
                pileId = TBridgePileId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId);
                const auto proxy = group.HasBridgeProxyGroupId()
                    ? StaticGroups.find(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId))
                    : StaticGroups.end();
                if (proxy != StaticGroups.end() && proxy->second.Info && proxy->second.Info->Group &&
                        proxy->second.Info->Group->HasBridgeGroupState()) {
                    state = &proxy->second.Info->Group->GetBridgeGroupState();
                }
            } else if (group.HasBridgeGroupState()) {
                state = &group.GetBridgeGroupState();
            } else if (!group.HasBridgeProxyGroupId()) {
                return NKikimrBridge::NOT_REQUIRED;
            }
        } else {
            return NKikimrBridge::UNKNOWN;
        }
        if (!BridgeInfo || BridgeInfo->IsImplicit) {
            return NKikimrBridge::UNKNOWN;
        }
        const NBridgeStorage::TGroupContext context(state);
        if (pileId) {
            const auto* part = state ? NBridge::GetGroupPile(*state, pileId) : nullptr;
            if (!part || TGroupId::FromProto(part, &NKikimrBridge::TGroupState::TPile::GetGroupId) != groupId) {
                return NKikimrBridge::UNKNOWN;
            }
            const auto* pile = BridgeInfo->FindPile(pileId);
            return pile && !pile->IsFreeSlot
                ? ObserveBridgeStorageCopy(context, *pile, FindPileSyncState(pileId), true).State
                : NKikimrBridge::UNKNOWN;
        }
        NBridgeStorage::TCounts counts;
        bool hasPiles = false;
        for (const auto& pile : BridgeInfo->Piles) {
            if (!pile.IsFreeSlot) {
                hasPiles = true;
                counts.Count(ObserveBridgeStorageCopy(context, pile, FindPileSyncState(pile.BridgePileId), true).State);
            }
        }
        return hasPiles ? counts.GetStatus() : NKikimrBridge::UNKNOWN;
    }

    Ydb::StatusIds::StatusCode TBlobStorageController::SelectBridgeStoragePools(const NKikimrBridge::TStorageQuery& query,
                                                                                std::set<TBoxStoragePoolId>& selected, NBridgeStorage::TReportBudget& budget, TString& error) const {
        if (query.StoragePoolIdsSize() || (query.GetFilterPools() && !query.PoolNamesSize() && !query.HasStoragePool())) {
            for (const auto& id : query.GetStoragePoolIds()) {
                if (!budget.Visit()) {
                    error = TString(LimitError);
                    return Ydb::StatusIds::PRECONDITION_FAILED;
                }
                if (!id.pool_id()) {
                    error = "Storage pool ID must be nonzero";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                const TBoxStoragePoolId key(id.box_id(), id.pool_id());
                if (selected.contains(key)) {
                    continue;
                }
                if (!StoragePools.contains(key)) {
                    error = "Storage pool not found";
                    return Ydb::StatusIds::NOT_FOUND;
                }
                selected.insert(key);
            }
            return Ydb::StatusIds::SUCCESS;
        }
        THashSet<TString> requested(query.GetPoolNames().begin(), query.GetPoolNames().end());
        THashMap<TString, ui32> matches;
        ui32 namedMatches = 0;
        for (const auto& [id, pool] : StoragePools) {
            if (!budget.Visit()) {
                error = TString(LimitError);
                return Ydb::StatusIds::PRECONDITION_FAILED;
            }
            if (query.GetFilterPools() && requested.contains(pool.Name)) {
                ++matches[pool.Name];
            }
            namedMatches += pool.Name == query.GetStoragePool();
            if ((!query.GetFilterPools() || requested.contains(pool.Name)) &&
                (!query.HasStoragePool() || pool.Name == query.GetStoragePool())) {
                selected.insert(id);
            }
        }
        for (const auto& name : requested) {
            if (query.GetFilterPools() && matches[name] != 1) {
                error = "Selected database storage pools must exist and have unique names";
                return Ydb::StatusIds::PRECONDITION_FAILED;
            }
        }
        if (query.HasStoragePool() && namedMatches != 1) {
            error = namedMatches ? "Storage pool name is ambiguous" : "Storage pool not found";
            return namedMatches ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::NOT_FOUND;
        }
        return Ydb::StatusIds::SUCCESS;
    }

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerQueryBridgeStorage::TPtr& ev) {
        using TQuery = NKikimrBridge::TStorageQuery;
        const auto& query = ev->Get()->Record;
        auto response = std::make_unique<TEvBlobStorage::TEvControllerQueryBridgeStorageResult>();
        auto& report = response->Record;
        auto reply = [&] { Send(ev->Sender, response.release(), 0, ev->Cookie); };
        auto fail = [&](Ydb::StatusIds::StatusCode status, const TString& message) {
            report.Clear(); // no successful prefix or truncation on any failure
            report.SetStatus(status);
            report.SetErrorMessage(message);
            reply();
        };
        if (!StorageConfig) {
            return fail(Ydb::StatusIds::UNAVAILABLE, "Storage configuration is unavailable");
        }
        if (!StorageConfig->HasClusterState()) {
            return fail(Ydb::StatusIds::NOT_FOUND, "Bridge cluster state is not configured");
        }
        if (!StorageConfig->HasSelfManagementConfig() || !StorageConfig->GetSelfManagementConfig().GetEnabled() ||
            !BridgeInfo || BridgeInfo->IsImplicit) {
            return fail(Ydb::StatusIds::PRECONDITION_FAILED, "Storage diagnostics require self-management and an explicit bridge configuration");
        }
        if (query.GetView() != TQuery::PILES && query.GetView() != TQuery::DATABASES && query.GetView() != TQuery::GROUPS) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Unknown storage diagnostic view");
        }
        if (query.GetGroupByStoragePool() && query.GetView() != TQuery::DATABASES) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Grouping by storage pool requires database synchronization view");
        }
        if (query.HasStoragePool() && query.GetStoragePool().empty()) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Storage pool name must not be empty");
        }
        if (query.StoragePoolIdsSize() && (query.HasStoragePool() || query.PoolNamesSize())) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Select storage pools by names or IDs");
        }
        if ((query.StatusesSize() && query.GetView() != TQuery::GROUPS) || (query.GetNotSynced() && query.GetView() != TQuery::DATABASES)) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Synchronization statuses filter groups; incomplete summaries filter databases");
        }
        if (query.HasStorageMode() && (query.GetView() != TQuery::DATABASES ||
                query.GetStorageMode() < NKikimrBridge::BRIDGED || query.GetStorageMode() > NKikimrBridge::MIXED)) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "storage_mode requires database synchronization and must be BRIDGED, ORDINARY, or MIXED");
        }
        ui32 statusMask = 0;
        for (const auto status : query.GetStatuses()) {
            if (status < NKikimrBridge::SYNCED || status > NKikimrBridge::UNKNOWN) {
                return fail(Ydb::StatusIds::BAD_REQUEST, "Unknown synchronization status");
            }
            statusMask |= 1u << status;
        }
        NBridgeStorage::TReportBudget budget;
        TVector<const TBridgeInfo::TPile*> piles;
        std::map<TBridgePileId, const NKikimrBridge::TClusterStateDetails::TPileSyncState*> syncStates;
        for (const auto& pile : BridgeInfo->Piles) {
            if (!budget.Visit()) {
                return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
            }
            if (!pile.IsFreeSlot && (!query.HasPileName() || pile.Name == query.GetPileName())) {
                piles.push_back(&pile);
                syncStates[pile.BridgePileId] = FindPileSyncState(pile.BridgePileId);
            }
        }
        if (query.HasPileName() && piles.empty()) {
            return fail(Ydb::StatusIds::NOT_FOUND, "Pile not found");
        }
        std::set<TBoxStoragePoolId> selected;
        TString selectionError;
        const auto selectionStatus = SelectBridgeStoragePools(query, selected, budget, selectionError);
        if (selectionStatus != Ydb::StatusIds::SUCCESS) {
            return fail(selectionStatus, selectionError);
        }
        report.SetStatus(Ydb::StatusIds::SUCCESS);
        const auto generation = StorageConfig->GetClusterState().GetGeneration();
        const auto observedAt = TActivationContext::Now();
        if (query.GetView() == TQuery::PILES) {
            report.MutablePiles()->set_cluster_state_generation(generation);
            SetTimestamp(observedAt, report.MutablePiles()->mutable_observed_at());
        } else if (query.GetView() == TQuery::DATABASES) {
            report.MutableDatabases()->set_cluster_state_generation(generation);
            SetTimestamp(observedAt, report.MutableDatabases()->mutable_observed_at());
        } else {
            report.MutableGroups()->set_cluster_state_generation(generation);
            SetTimestamp(observedAt, report.MutableGroups()->mutable_observed_at());
        }
        std::map<TBridgePileId, NBridgeStorage::TCounts> databaseCounts;
        std::map<std::pair<TPathId, TBoxStoragePoolId>, TOwnerSummary> owners;
        auto resolveOrdinaryPlacement = [&](const TGroupInfo& group, const NBridgeStorage::TGroupContext& context,
                std::set<TBridgePileId>& placement, std::optional<ui32>& physical) {
            const TGroupInfo* actual = &group;
            if (context.AuthorityPileId) {
                const auto* part = NBridge::GetGroupPile(*context.State, context.AuthorityPileId);
                actual = part ? FindGroup(TGroupId::FromValue(part->GetGroupId())) : nullptr;
            }
            if (!actual) {
                return true;
            }
            physical = actual->ID.GetRawId();
            if (actual->VDisksInGroup.empty()) {
                return true;
            }
            for (const TVSlotInfo* slot : actual->VDisksInGroup) {
                if (!budget.Visit()) {
                    return false;
                }
                const auto* pile = slot ? BridgeInfo->GetPileForNode(slot->VSlotId.NodeId) : nullptr;
                if (!pile || pile->IsFreeSlot) {
                    placement.clear(); // partial knowledge must not look like complete placement
                    return true;
                }
                placement.insert(pile->BridgePileId);
            }
            return true;
        };
        ui64 unattributedPools = 0;
        const bool scopedDatabase = query.GetView() == TQuery::DATABASES && query.GetFilterPools();
        for (const auto& poolId : selected) {
            const auto& pool = StoragePools.at(poolId);
            TOwnerSummary* owner = nullptr;
            if (query.GetView() != TQuery::GROUPS) {
                const TPathId path(pool.SchemeshardId.GetOrElse(0), pool.PathItemId.GetOrElse(0));
                const bool known = path.OwnerId && path.LocalPathId;
                if (!known && query.GetView() == TQuery::PILES) {
                    ++unattributedPools;
                    continue; // a pool without an owner is not a database
                }
                // A database selected by path already supplies its complete pool list.
                const auto ownerPath = known && (!scopedDatabase || query.GetGroupByStoragePool()) ? path : TPathId(0, 0);
                const auto fallbackPool = query.GetGroupByStoragePool() || (!known && !scopedDatabase)
                    ? poolId : TBoxStoragePoolId{};
                owner = &owners[{ownerPath, fallbackPool}];
                const auto mode = pool.BridgeMode ? NKikimrBridge::BRIDGED : NKikimrBridge::ORDINARY;
                owner->Mode = owner->Mode == NKikimrBridge::STORAGE_MODE_UNSPECIFIED ? mode : owner->Mode == mode ? mode
                                                                                                                : NKikimrBridge::MIXED;
                if (query.GetView() == TQuery::DATABASES) {
                    owner->Pools.push_back(poolId);
                    if (!budget.Fits(pool.Name.size() + 64) || !budget.Fits(owner->PoolBytes + pool.Name.size() + 64)) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    owner->PoolBytes += pool.Name.size() + 64;
                }
                if (pool.BridgeMode) {
                    for (const auto* pile : piles) {
                        if (!budget.Visit()) {
                            return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                        }
                        owner->Copies.try_emplace(pile->BridgePileId);
                    }
                }
            }
            if (!pool.BridgeMode && ((query.GetView() == TQuery::GROUPS && statusMask &&
                    !(statusMask & (1u << NKikimrBridge::NOT_REQUIRED))) ||
                    (query.GetView() == TQuery::DATABASES && query.GetNotSynced()))) {
                continue;
            }
            NKikimrBridge::TStoragePool reportedPool;
            bool poolFilled = false;
            for (auto it = StoragePoolGroups.lower_bound({poolId, TGroupId::Zero()});
                 it != StoragePoolGroups.end() && it->first == poolId; ++it) {
                if (!budget.Visit()) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                const auto* group = FindGroup(it->second);
                if (!group || ((group->BridgePileId || group->VirtualGroupState) && !group->BridgeGroupInfo)) {
                    continue;
                }
                const NBridgeStorage::TGroupContext context(group->BridgeGroupInfo ? &group->BridgeGroupInfo->GetBridgeGroupState() : nullptr);
                TVector<const TBridgeInfo::TPile*> ordinaryPiles;
                const auto* groupPiles = &piles;
                std::optional<ui32> ordinaryPhysical;
                if (!pool.BridgeMode) {
                    std::set<TBridgePileId> placement;
                    if (!resolveOrdinaryPlacement(*group, context, placement, ordinaryPhysical)) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    if (placement.empty()) {
                        if (!query.HasPileName()) {
                            ordinaryPiles.push_back(nullptr);
                        }
                    } else {
                        for (const auto id : placement) {
                            const auto* pile = BridgeInfo->GetPile(id);
                            if (!query.HasPileName() || pile->Name == query.GetPileName()) {
                                ordinaryPiles.push_back(pile);
                            }
                        }
                    }
                    groupPiles = &ordinaryPiles;
                }
                for (const auto* pile : *groupPiles) {
                    if (!budget.Visit()) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    NBridgeStorage::TCopyObservation copy;
                    if (pool.BridgeMode) {
                        copy = ObserveBridgeStorageCopy(context, *pile, syncStates.at(pile->BridgePileId), true);
                    } else {
                        copy.State = NKikimrBridge::NOT_REQUIRED;
                    }
                    if (owner) {
                        owner->Copies[pile ? pile->BridgePileId : TBridgePileId()].Add(copy);
                    } else if (!statusMask || (statusMask & (1u << copy.State))) {
                        if (!poolFilled) {
                            if (!budget.Fits(pool.Name.size() + 64)) {
                                return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                            }
                            CopyPool(poolId, pool.Name, &reportedPool);
                            poolFilled = true;
                        }
                        const auto physical = !pool.BridgeMode ? ordinaryPhysical :
                            copy.Part ? std::optional<ui32>(copy.Part->GetGroupId()) : std::nullopt;
                        if (!NBridgeStorage::AppendGroup({group->ID.GetRawId(), physical, &reportedPool,
                                                          pile ? std::optional<TStringBuf>(pile->Name) : std::nullopt, copy}, budget, *report.MutableGroups())) {
                            return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                        }
                    }
                }
            }
        }
        if (query.GetView() == TQuery::GROUPS && !query.HasStoragePool() && !query.StoragePoolIdsSize() && !query.GetFilterPools()) {
            for (const auto& [id, group] : StaticGroups) {
                if (!budget.Visit()) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                if (!group.Info || !group.Info->Group) {
                    continue;
                }
                const auto& proto = *group.Info->Group;
                if (!proto.HasBridgeGroupState() && proto.HasBridgeProxyGroupId() &&
                        TGroupId::FromProto(&proto, &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId) != id) {
                    continue; // physical parts are represented through their logical group
                }
                if (proto.HasBridgeGroupState()) {
                    const NBridgeStorage::TGroupContext context(&proto.GetBridgeGroupState());
                    for (const auto* pile : piles) {
                        if (!budget.Visit()) {
                            return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                        }
                        const auto copy = ObserveBridgeStorageCopy(context, *pile, syncStates.at(pile->BridgePileId), true);
                        if (!statusMask || (statusMask & (1u << copy.State))) {
                            const auto physical = copy.Part ? std::optional<ui32>(copy.Part->GetGroupId()) : std::nullopt;
                            if (!NBridgeStorage::AppendGroup({id.GetRawId(), physical, nullptr, TStringBuf(pile->Name), copy},
                                    budget, *report.MutableGroups())) {
                                return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                            }
                        }
                    }
                    continue;
                }
                if (statusMask && !(statusMask & (1u << NKikimrBridge::NOT_REQUIRED))) {
                    continue;
                }
                const auto& members = group.Info->GetDynamicInfo().ServiceIdForOrderNumber;
                std::set<TBridgePileId> placement;
                bool complete = !members.empty();
                for (const auto& member : members) {
                    if (!budget.Visit()) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    const auto* pile = BridgeInfo->GetPileForNode(member.NodeId());
                    if (!pile || pile->IsFreeSlot) {
                        complete = false;
                        break;
                    }
                    placement.insert(pile->BridgePileId);
                }
                if (!complete) {
                    placement.clear();
                }
                TVector<const TBridgeInfo::TPile*> actualPiles;
                if (placement.empty() && !query.HasPileName()) {
                    actualPiles.push_back(nullptr);
                }
                for (const auto pileId : placement) {
                    const auto* pile = BridgeInfo->GetPile(pileId);
                    if (!query.HasPileName() || pile->Name == query.GetPileName()) {
                        actualPiles.push_back(pile);
                    }
                }
                for (const auto* pile : actualPiles) {
                    if (!budget.Visit() || !NBridgeStorage::AppendGroup({id.GetRawId(), id.GetRawId(), nullptr,
                            pile ? std::optional<TStringBuf>(pile->Name) : std::nullopt, {.State = NKikimrBridge::NOT_REQUIRED}},
                            budget, *report.MutableGroups())) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                }
            }
        }
        if (query.GetView() == TQuery::PILES) {
            // A pool filter can hide other pools of the same owner and change its apparent mode.
            const bool reportBridgeDatabases = !query.HasStoragePool() && !query.StoragePoolIdsSize() && !query.GetFilterPools();
            ui64 bridgeDatabases = 0;
            std::map<TBridgePileId, NBridgeStorage::TCounts> bridgeDatabaseCounts;
            for (auto& [key, owner] : owners) {
                Y_UNUSED(key);
                if (!budget.Visit()) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                if (owner.Mode == NKikimrBridge::ORDINARY && owner.Copies.empty() && !query.HasPileName()) {
                    owner.Copies.try_emplace(TBridgePileId());
                }
                const bool bridged = reportBridgeDatabases && owner.Mode == NKikimrBridge::BRIDGED;
                bridgeDatabases += bridged;
                for (const auto& [pileId, copy] : owner.Copies) {
                    if (!pileId && owner.Mode == NKikimrBridge::MIXED) {
                        continue;
                    }
                    if (!budget.Visit()) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    const auto status = copy.Counts.GetStatus();
                    databaseCounts[pileId].CountDatabase(status);
                    if (bridged) {
                        bridgeDatabaseCounts[pileId].CountDatabase(status);
                    }
                }
            }
            report.MutablePiles()->set_unattributed_storage_pools(unattributedPools);
            if (reportBridgeDatabases) {
                report.MutablePiles()->set_bridge_databases(bridgeDatabases);
            }
            for (const auto* pile : piles) {
                if (!budget.Visit() || !budget.Fits(pile->Name.size() + 128)) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                NKikimrBridge::TPileSynchronization row;
                row.set_pile_name(pile->Name);
                databaseCounts[pile->BridgePileId].ToProto(row.mutable_databases());
                if (reportBridgeDatabases) {
                    bridgeDatabaseCounts[pile->BridgePileId].ToProto(row.mutable_bridge_databases());
                }
                if (!budget.Account(row)) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                report.MutablePiles()->add_piles()->Swap(&row);
            }
            const auto unplaced = databaseCounts.find(TBridgePileId());
            if (unplaced != databaseCounts.end() && unplaced->second.Total) {
                NKikimrBridge::TPileSynchronization row;
                unplaced->second.ToProto(row.mutable_databases());
                if (!budget.Visit() || !budget.Account(row)) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                report.MutablePiles()->add_piles()->Swap(&row); // no pile name for unresolved placement
            }
        } else if (query.GetView() == TQuery::DATABASES) {
            for (auto& [key, owner] : owners) {
                if (query.HasStorageMode() && query.GetStorageMode() != owner.Mode) {
                    continue; // match the selected owner or per-pool summary mode
                }
                if (owner.Mode == NKikimrBridge::ORDINARY && owner.Copies.empty() && !query.HasPileName()) {
                    owner.Copies.try_emplace(TBridgePileId());
                }
                for (const auto& [pileId, copy] : owner.Copies) {
                    if (!pileId && owner.Mode == NKikimrBridge::MIXED) {
                        continue; // ordinary placement uncertainty is not a bridge synchronization row
                    }
                    if (!budget.Visit()) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    if (query.GetNotSynced() && copy.Counts.Synced == copy.Counts.Total) {
                        continue;
                    }
                    const auto* pile = pileId ? BridgeInfo->GetPile(pileId) : nullptr;
                    const ui64 errorBytes = copy.LastError.LastError ? copy.LastError.LastError->size() : 0;
                    const ui64 pileBytes = pile ? pile->Name.size() : 0;
                    if (!budget.Fits(errorBytes) || !budget.Fits(pileBytes) ||
                        !budget.Fits(owner.PoolBytes + errorBytes + pileBytes + 128)) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    NKikimrBridge::TDatabaseSynchronization row;
                    row.set_storage_mode(owner.Mode);
                    for (const auto& poolId : owner.Pools) {
                        CopyPool(poolId, StoragePools.at(poolId).Name, row.add_storage_pools());
                    }
                    if (pile) {
                        row.set_pile_name(pile->Name);
                    }
                    copy.Counts.ToProto(row.mutable_groups());
                    if (copy.LastError.LastError && (!copy.LastError.LastError->empty() || copy.LastError.LastErrorTimestamp)) {
                        row.mutable_last_sync_error()->set_message(*copy.LastError.LastError);
                        if (copy.LastError.LastErrorTimestamp) {
                            SetTimestamp(copy.LastError.LastErrorTimestamp, row.mutable_last_sync_error()->mutable_occurred_at());
                        }
                    }
                    if (!budget.Account(row)) {
                        return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                    }
                    if (key.first.OwnerId && key.first.LocalPathId) {
                        auto* annotation = report.AddOwners();
                        annotation->SetRowIndex(report.GetDatabases().databases_size());
                        annotation->SetOwnerSchemeShardId(key.first.OwnerId);
                        annotation->SetOwnerPathId(key.first.LocalPathId);
                        if (!budget.Account(*annotation)) {
                            return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                        }
                    }
                    report.MutableDatabases()->add_databases()->Swap(&row);
                }
            }
        }
        if (report.ByteSizeLong() > TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxResultBytes) {
            return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
        }
        reply();
    }

} // namespace NKikimr::NBsController
