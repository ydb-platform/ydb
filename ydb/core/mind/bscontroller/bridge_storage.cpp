#include "impl.h"
#include "bridge_storage.h"

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
            result.State = Ydb::Bridge::NOT_REQUIRED;
        } else if (!context.Group.State) {
            result.State = Ydb::Bridge::UNKNOWN;
        } else if (!result.Part || !result.CurrentGeneration) {
            result.State = Ydb::Bridge::PENDING;
        } else if (!authority && (context.PileState == NKikimrBridge::TClusterState::DISCONNECTED ||
                                  context.PileState == NKikimrBridge::TClusterState::SUSPENDED)) {
            result.State = Ydb::Bridge::UNKNOWN;
        } else if (result.Part->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
            result.State = Ydb::Bridge::SYNCED;
        } else {
            result.State = context.SyncerRunning ? Ydb::Bridge::SYNCING : Ydb::Bridge::PENDING;
        }
        return result;
    }

    void TCounts::Count(ECopyState state) {
        if (state == Ydb::Bridge::NOT_REQUIRED) {
            return;
        }
        ++Total;
        switch (state) {
            case Ydb::Bridge::SYNCED:
                ++Synced;
                break;
            case Ydb::Bridge::SYNCING:
                ++Running;
                break;
            case Ydb::Bridge::PENDING:
                ++Pending;
                break;
            default:
                ++Unknown;
                break;
        }
    }

    void TCounts::ToProto(Ydb::Bridge::GroupSyncCounts* to) const {
        to->set_required(Total);
        to->set_synced(Synced);
        to->set_syncing(Running);
        to->set_pending(Pending);
        to->set_unknown(Unknown);
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

        void CopyError(const TCopyObservation& copy, Ydb::Bridge::SynchronizationError* to) {
            to->set_message(*copy.LastError);
            if (copy.LastErrorTimestamp) {
                SetTimestamp(copy.LastErrorTimestamp, to->mutable_occurred_at());
            }
        }
    } // namespace

    bool AppendGroup(const TGroupRow& input, TReportBudget& budget, Ydb::Bridge::ListGroupSynchronizationResult& result) {
        const ui64 errorBytes = input.Copy.LastError ? input.Copy.LastError->size() : 0;
        const ui64 pileBytes = input.PileName ? input.PileName->size() : 0;
        if (!budget.Fits(errorBytes) || !budget.Fits(pileBytes) || !budget.Fits(input.Pool.name().size()) ||
            !budget.Fits(errorBytes + pileBytes + input.Pool.name().size() + 128)) {
            return false; // reject before copying an arbitrarily long upstream error
        }
        Ydb::Bridge::GroupSynchronization row;
        row.set_group_id(input.GroupId);
        if (input.PhysicalGroupId) {
            row.set_physical_group_id(*input.PhysicalGroupId);
        }
        row.mutable_storage_pool()->CopyFrom(input.Pool);
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

        void CopyPool(TBoxStoragePoolId id, const TString& name, Ydb::Bridge::StoragePool* to) {
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
            Ydb::Bridge::StorageMode Mode = Ydb::Bridge::STORAGE_MODE_UNSPECIFIED;
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
        if (copy.Part && copy.CurrentGeneration && copy.State != Ydb::Bridge::NOT_REQUIRED) {
            const auto partId = TGroupId::FromProto(copy.Part, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (const auto it = BridgeSyncState.find(partId); it != BridgeSyncState.end() && it->second.Stage == copy.Part->GetStage()) {
                copy.LastError = &it->second.LastError;
                copy.LastErrorTimestamp = it->second.LastErrorTimestamp;
            }
        }
        return copy;
    }

    Ydb::StatusIds::StatusCode TBlobStorageController::SelectBridgeStoragePools(const NKikimrBridge::TStorageQuery& query,
                                                                                std::set<TBoxStoragePoolId>& selected, NBridgeStorage::TReportBudget& budget, TString& error) const {
        if (query.HasStoragePoolId()) {
            const auto& id = query.GetStoragePoolId();
            const TBoxStoragePoolId key(id.box_id(), id.pool_id());
            if (!StoragePools.contains(key)) {
                error = "Storage pool not found";
                return Ydb::StatusIds::NOT_FOUND;
            }
            selected.insert(key);
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
                (query.GetStoragePool().empty() || pool.Name == query.GetStoragePool())) {
                selected.insert(id);
            }
        }
        for (const auto& name : requested) {
            if (query.GetFilterPools() && matches[name] != 1) {
                error = "Selected database storage pools must exist and have unique names";
                return Ydb::StatusIds::PRECONDITION_FAILED;
            }
        }
        if (!query.GetStoragePool().empty() && namedMatches != 1) {
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
        if (query.HasStoragePoolId() && (!query.GetStoragePoolId().pool_id() || !query.GetStoragePool().empty() || query.GetFilterPools())) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Select storage pools by database, name, or a nonzero pool ID");
        }
        if ((query.StatusesSize() && query.GetView() != TQuery::GROUPS) || (query.GetNotSynced() && query.GetView() != TQuery::DATABASES)) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Synchronization statuses filter groups; incomplete summaries filter databases");
        }
        ui32 statusMask = 0;
        for (const auto status : query.GetStatuses()) {
            if (status < Ydb::Bridge::SYNCED || status > Ydb::Bridge::UNKNOWN) {
                return fail(Ydb::StatusIds::BAD_REQUEST, "Unknown synchronization status");
            }
            statusMask |= 1u << status;
        }
        if (query.GetView() == TQuery::GROUPS && !query.HasStoragePoolId() && query.GetStoragePool().empty() && !query.GetFilterPools()) {
            return fail(Ydb::StatusIds::BAD_REQUEST, "Group diagnostics require a database or storage pool filter");
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
        std::map<TBridgePileId, NBridgeStorage::TCounts> userCounts, systemCounts;
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
        ui64 ordinaryGroups = 0;
        for (const auto& poolId : selected) {
            const auto& pool = StoragePools.at(poolId);
            TOwnerSummary* owner = nullptr;
            if (query.GetView() == TQuery::DATABASES) {
                const TPathId path(pool.SchemeshardId.GetOrElse(0), pool.PathItemId.GetOrElse(0));
                const bool known = path.OwnerId && path.LocalPathId;
                owner = &owners[{known ? path : TPathId(0, 0), known ? TBoxStoragePoolId{} : poolId}];
                const auto mode = pool.BridgeMode ? Ydb::Bridge::BRIDGED : Ydb::Bridge::ORDINARY;
                owner->Mode = owner->Mode == Ydb::Bridge::STORAGE_MODE_UNSPECIFIED ? mode : owner->Mode == mode ? mode
                                                                                                                : Ydb::Bridge::MIXED;
                owner->Pools.push_back(poolId);
                if (!budget.Fits(pool.Name.size() + 64) || !budget.Fits(owner->PoolBytes + pool.Name.size() + 64)) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                owner->PoolBytes += pool.Name.size() + 64;
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
                    !(statusMask & (1u << Ydb::Bridge::NOT_REQUIRED))) ||
                    (query.GetView() == TQuery::DATABASES && query.GetNotSynced()))) {
                continue;
            }
            Ydb::Bridge::StoragePool publicPool;
            bool poolFilled = false;
            for (auto it = StoragePoolGroups.lower_bound({poolId, TGroupId::Zero()});
                 it != StoragePoolGroups.end() && it->first == poolId; ++it) {
                if (!budget.Visit()) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                const auto* group = FindGroup(it->second);
                if (!group || (group->BridgePileId && !group->BridgeGroupInfo) || group->VirtualGroupState) {
                    continue;
                }
                if (!pool.BridgeMode && query.GetView() == TQuery::PILES) {
                    ++ordinaryGroups;
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
                        copy.State = Ydb::Bridge::NOT_REQUIRED;
                    }
                    if (query.GetView() == TQuery::PILES) {
                        userCounts[pile->BridgePileId].Count(copy.State);
                    } else if (owner) {
                        owner->Copies[pile ? pile->BridgePileId : TBridgePileId()].Add(copy);
                    } else if (!statusMask || (statusMask & (1u << copy.State))) {
                        if (!poolFilled) {
                            if (!budget.Fits(pool.Name.size() + 64)) {
                                return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                            }
                            CopyPool(poolId, pool.Name, &publicPool);
                            poolFilled = true;
                        }
                        const auto physical = !pool.BridgeMode ? ordinaryPhysical :
                            copy.Part ? std::optional<ui32>(copy.Part->GetGroupId()) : std::nullopt;
                        if (!NBridgeStorage::AppendGroup({group->ID.GetRawId(), physical, publicPool,
                                                          pile ? std::optional<TStringBuf>(pile->Name) : std::nullopt, copy}, budget, *report.MutableGroups())) {
                            return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                        }
                    }
                }
            }
        }
        if (query.GetView() == TQuery::PILES) {
            report.MutablePiles()->set_ordinary_groups(ordinaryGroups);
            for (const auto& [id, group] : StaticGroups) {
                Y_UNUSED(id);
                if (!budget.Visit()) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                if (group.Info && group.Info->Group && group.Info->Group->HasBridgeGroupState()) {
                    const NBridgeStorage::TGroupContext context(&group.Info->Group->GetBridgeGroupState());
                    for (const auto* pile : piles) {
                        if (!budget.Visit()) {
                            return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                        }
                        systemCounts[pile->BridgePileId].Count(ObserveBridgeStorageCopy(context, *pile,
                                                                                        syncStates.at(pile->BridgePileId), true)
                                                                   .State);
                    }
                }
            }
            for (const auto* pile : piles) {
                if (!budget.Fits(pile->Name.size() + 128)) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                Ydb::Bridge::PileSynchronization row;
                row.set_pile_name(pile->Name);
                userCounts[pile->BridgePileId].ToProto(row.mutable_user_groups());
                systemCounts[pile->BridgePileId].ToProto(row.mutable_system_groups());
                if (!budget.Account(row)) {
                    return fail(Ydb::StatusIds::PRECONDITION_FAILED, TString(LimitError));
                }
                report.MutablePiles()->add_piles()->Swap(&row);
            }
        } else if (query.GetView() == TQuery::DATABASES) {
            for (auto& [key, owner] : owners) {
                if (owner.Mode == Ydb::Bridge::ORDINARY && owner.Copies.empty() && !query.HasPileName()) {
                    owner.Copies.try_emplace(TBridgePileId());
                }
                for (const auto& [pileId, copy] : owner.Copies) {
                    if (!pileId && owner.Mode == Ydb::Bridge::MIXED) {
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
                    Ydb::Bridge::DatabaseSynchronization row;
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
