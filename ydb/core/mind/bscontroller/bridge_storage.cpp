#include "impl.h"
#include "bridge_storage.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <algorithm>

namespace NKikimr::NBsController::NBridgeStorage {

    TCopyObservation ObserveCopy(const TCopyContext& context) {
        using TGroup = NKikimrBridge::TStorageGroup;
        TCopyObservation result;
        const bool required = context.BridgeMode &&
                              context.PileState != NKikimrBridge::TClusterState::INACTIVE;
        if (context.Group.State) {
            result.Part = NBridge::GetGroupPile(*context.Group.State, context.PileId);
        }
        const bool authority = context.Group.AuthorityPileId == context.PileId;
        result.CurrentGeneration = authority || !context.PileSyncState || (result.Part && result.Part->GetBecameUnsyncedGeneration() >= context.PileSyncState->GetBecameUnsyncedGeneration());
        if (!required) {
            result.State = TGroup::NOT_REQUIRED;
        } else if (!context.Group.State) {
            result.State = TGroup::UNKNOWN;
        } else if (!result.Part || !result.CurrentGeneration) {
            result.State = TGroup::PENDING;
        } else if (!authority && (context.PileState == NKikimrBridge::TClusterState::DISCONNECTED ||
                                  context.PileState == NKikimrBridge::TClusterState::SUSPENDED)) {
            result.State = TGroup::UNKNOWN;
        } else if (result.Part->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
            result.State = TGroup::SYNCED;
        } else {
            result.State = context.SyncerRunning ? TGroup::SYNCING : TGroup::PENDING;
        }
        return result;
    }

    void TCounts::Count(ECopyState state) {
        using TGroup = NKikimrBridge::TStorageGroup;
        if (state == TGroup::NOT_REQUIRED) {
            return;
        }
        ++Total;
        switch (state) {
            case TGroup::SYNCED:
                ++Synced;
                break;
            case TGroup::SYNCING:
                ++Running;
                break;
            case TGroup::PENDING:
                ++Pending;
                break;
            default:
                ++Unknown;
                break;
        }
    }

    void TCounts::Add(const TCounts& other) {
        Total += other.Total;
        Synced += other.Synced;
        Running += other.Running;
        Pending += other.Pending;
        Unknown += other.Unknown;
    }

    ECopyState TCounts::State() const {
        using TGroup = NKikimrBridge::TStorageGroup;
        if (!Total) {
            return TGroup::NOT_REQUIRED;
        } else if (Synced == Total) {
            return TGroup::SYNCED;
        } else if (Running) {
            return TGroup::SYNCING;
        } else if (Unknown) {
            return TGroup::UNKNOWN;
        }
        return TGroup::PENDING;
    }

    void TCounts::ToProto(NKikimrBridge::TStorageCounts* to) const {
        to->SetTotal(Total);
        to->SetSynced(Synced);
        to->SetRunning(Running);
        to->SetPending(Pending);
        to->SetUnknown(Unknown);
    }

    void TCopySummary::Add(const TCopyObservation& copy) {
        Groups.Count(copy.State);
        if (LastErrorTimestamp < copy.LastErrorTimestamp && copy.LastError) {
            LastError = *copy.LastError;
            LastErrorTimestamp = copy.LastErrorTimestamp;
        }
    }

    void TCopySummary::Add(const TCopySummary& copy) {
        Groups.Add(copy.Groups);
        if (LastErrorTimestamp < copy.LastErrorTimestamp) {
            LastError = copy.LastError;
            LastErrorTimestamp = copy.LastErrorTimestamp;
        }
    }

    TDatabaseSummaries BuildDatabaseSummaries(const TPoolSummaries& pools,
                                              const std::set<TBoxStoragePoolId>* selected) {
        struct TOwnerSummary {
            TVector<TString> StoragePools;
            EMode Mode = NKikimrBridge::TStorageDatabase::MODE_UNSPECIFIED;
            std::map<TBridgePileId, TCopySummary> Copies;
        };
        std::map<TOwnerKey, TOwnerSummary> owners;
        for (const auto& [poolId, pool] : pools) {
            if (selected && !selected->contains(poolId)) {
                continue;
            }
            const bool knownOwner = pool.Owner.OwnerId && pool.Owner.LocalPathId;
            auto& owner = owners[{pool.Owner, knownOwner ? TBoxStoragePoolId{} : poolId}];
            owner.StoragePools.push_back(pool.Name);
            if (owner.Mode == NKikimrBridge::TStorageDatabase::MODE_UNSPECIFIED) {
                owner.Mode = pool.Mode;
            } else if (owner.Mode != pool.Mode) {
                owner.Mode = NKikimrBridge::TStorageDatabase::MIXED;
            }
            if (pool.Mode == NKikimrBridge::TStorageDatabase::BRIDGED) {
                for (const auto& [pileId, copy] : pool.Copies) {
                    owner.Copies[pileId].Add(copy);
                }
            }
        }
        TDatabaseSummaries result;
        for (auto& [key, owner] : owners) {
            if (owner.Mode == NKikimrBridge::TStorageDatabase::ORDINARY) {
                result.emplace(TDatabaseKey{key, {}}, TDatabaseSummary{std::move(owner.StoragePools), owner.Mode, {}});
            } else {
                for (auto& [pileId, copy] : owner.Copies) {
                    result.emplace(TDatabaseKey{key, pileId}, TDatabaseSummary{owner.StoragePools, owner.Mode, std::move(copy)});
                }
            }
        }
        return result;
    }

    ui64 QueryHash(const NKikimrBridge::TStorageQuery& query) {
        auto signature = query;
        signature.ClearPageToken();
        signature.ClearPageSize();
        std::sort(signature.MutablePoolNames()->begin(), signature.MutablePoolNames()->end());
        return THash<TString>{}(TString(signature.SerializeAsString()));
    }

    bool ParsePageToken(const NKikimrBridge::TStorageQuery& query, ui64 generation,
                        NKikimrBridge::TStoragePageToken& token) {
        token.Clear();
        if (query.GetPageToken().empty()) {
            return true;
        }
        if (query.GetPageToken().size() > 1024) {
            return false;
        }
        try {
            if (!token.ParseFromString(Base64Decode(query.GetPageToken())) || token.GetGeneration() != generation ||
                token.GetQueryHash() != QueryHash(query)) {
                return false;
            }
        } catch (const std::exception&) {
            return false;
        }
        if (query.GetView() == NKikimrBridge::TStorageQuery::DATABASES && token.HasDatabase()) {
            const auto& cursor = token.GetDatabase();
            const bool knownOwner = cursor.GetOwnerSchemeShardId() && cursor.GetOwnerPathId();
            const bool unknownOwner = !cursor.GetOwnerSchemeShardId() && !cursor.GetOwnerPathId();
            return (knownOwner && !cursor.GetBoxId() && !cursor.GetStoragePoolId()) ||
                   (unknownOwner && cursor.GetStoragePoolId());
        }
        if (query.GetView() == NKikimrBridge::TStorageQuery::GROUPS && token.HasGroup()) {
            const auto& cursor = token.GetGroup();
            return cursor.GetStoragePoolId() && cursor.GetGroupId() &&
                   (!cursor.HasLastPileId() || cursor.GetLastPileId());
        }
        return false;
    }

    TString MakePageToken(const NKikimrBridge::TStorageQuery& query, ui64 generation, const TDatabaseKey& last) {
        NKikimrBridge::TStoragePageToken token;
        token.SetGeneration(generation);
        token.SetQueryHash(QueryHash(query));
        auto* cursor = token.MutableDatabase();
        cursor->SetOwnerSchemeShardId(last.Owner.PathId.OwnerId);
        cursor->SetOwnerPathId(last.Owner.PathId.LocalPathId);
        cursor->SetBoxId(std::get<0>(last.Owner.FallbackPool));
        cursor->SetStoragePoolId(std::get<1>(last.Owner.FallbackPool));
        cursor->SetPileId(last.PileId.GetLocalDb());
        return Base64Encode(token.SerializeAsString());
    }

    TString MakePageToken(const NKikimrBridge::TStorageQuery& query, ui64 generation,
                          const NKikimrBridge::TStoragePageToken::TGroupCursor& last) {
        NKikimrBridge::TStoragePageToken token;
        token.SetGeneration(generation);
        token.SetQueryHash(QueryHash(query));
        token.MutableGroup()->CopyFrom(last);
        return Base64Encode(token.SerializeAsString());
    }

    TDatabaseKey DatabaseCursorKey(const NKikimrBridge::TStoragePageToken::TDatabaseCursor& cursor) {
        return {{TPathId(cursor.GetOwnerSchemeShardId(), cursor.GetOwnerPathId()),
                 {cursor.GetBoxId(), cursor.GetStoragePoolId()}},
                TBridgePileId::FromLocalDb(cursor.GetPileId())};
    }

} // namespace NKikimr::NBsController::NBridgeStorage

namespace NKikimr::NBsController {

    namespace {

        constexpr ui32 MaxStorageScan = 10000;
        const TString NoPileName;

        ui32 StoragePageSize(const NKikimrBridge::TStorageQuery& query) {
            return query.GetPageSize() ? query.GetPageSize() : 1000;
        }

        bool IncludeStorageCopy(const NKikimrBridge::TStorageQuery& query, NBridgeStorage::ECopyState state) {
            return !query.GetNotSynced() || (state != NKikimrBridge::TStorageGroup::SYNCED &&
                                             state != NKikimrBridge::TStorageGroup::NOT_REQUIRED);
        }

        bool FilterStoragePools(const NKikimrBridge::TStorageQuery& query) {
            return query.GetFilterPools() || !query.GetStoragePool().empty();
        }

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
        if (copy.Part && copy.CurrentGeneration && copy.State != NKikimrBridge::TStorageGroup::NOT_REQUIRED) {
            const auto partId = TGroupId::FromProto(copy.Part, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (const auto it = BridgeSyncState.find(partId); it != BridgeSyncState.end() &&
                                                              it->second.Stage == copy.Part->GetStage()) {
                copy.LastError = &it->second.LastError;
                copy.LastErrorTimestamp = it->second.LastErrorTimestamp;
            }
        }
        return copy;
    }

    NKikimrBridge::TStorageGroup TBlobStorageController::DescribeBridgeStorageGroup(TGroupId groupId,
                                                                                    const TString& pool, const TString& pile, const NBridgeStorage::TCopyObservation& copy) const {
        NKikimrBridge::TStorageGroup row;
        row.SetGroupId(groupId.GetRawId());
        row.SetPoolName(pool);
        row.SetPileName(pile);
        row.SetState(copy.State);
        if (copy.Part) {
            row.SetPartGroupId(copy.Part->GetGroupId());
            row.SetStage(NKikimrBridge::TGroupState::EStage_Name(copy.Part->GetStage()));
        }
        if (copy.LastError) {
            row.SetLastError(*copy.LastError);
            row.SetLastErrorTimestampUs(copy.LastErrorTimestamp.MicroSeconds());
        }
        return row;
    }

    void TBlobStorageController::RefreshBridgeStorage() {
        if (!StorageConfig || !BridgeInfo || BridgeInfo->IsImplicit) {
            return;
        }
        NBridgeStorage::TSnapshot snapshot;
        snapshot.Generation = StorageConfig->GetClusterState().GetGeneration();
        snapshot.CollectedAt = TActivationContext::Now();
        std::map<TBridgePileId, const NKikimrBridge::TClusterStateDetails::TPileSyncState*> syncStates;
        for (const auto& pile : BridgeInfo->Piles) {
            if (!pile.IsFreeSlot) {
                snapshot.Piles[pile.BridgePileId].Name = pile.Name;
                syncStates[pile.BridgePileId] = FindPileSyncState(pile.BridgePileId);
            }
        }
        for (const auto& [poolId, pool] : StoragePools) {
            auto& summary = snapshot.Pools[poolId];
            summary.Id = poolId;
            summary.Name = pool.Name;
            summary.Owner = TPathId(pool.SchemeshardId.GetOrElse(0), pool.PathItemId.GetOrElse(0));
            summary.Mode = pool.BridgeMode ? NKikimrBridge::TStorageDatabase::BRIDGED : NKikimrBridge::TStorageDatabase::ORDINARY;
            for (const auto& [pileId, pile] : snapshot.Piles) {
                Y_UNUSED(pile);
                summary.Copies.try_emplace(pileId);
            }
        }
        for (const auto& [groupId, group] : GroupMap) {
            Y_UNUSED(groupId);
            if ((group->BridgePileId && !group->BridgeGroupInfo) || group->VirtualGroupState) {
                continue; // logical groups, excluding physical parts and virtual indirections
            }
            const auto pool = snapshot.Pools.find(group->StoragePoolId);
            if (pool == snapshot.Pools.end() || pool->second.Mode != NKikimrBridge::TStorageDatabase::BRIDGED) {
                continue; // ordinary pools have no replication requirement
            }
            const NBridgeStorage::TGroupContext context(group->BridgeGroupInfo ? &group->BridgeGroupInfo->GetBridgeGroupState() : nullptr);
            for (auto& [pileId, summary] : snapshot.Piles) {
                const auto copy = ObserveBridgeStorageCopy(context, *BridgeInfo->GetPile(pileId), syncStates.at(pileId), true);
                pool->second.Copies.at(pileId).Add(copy);
                summary.UserGroups.Count(copy.State);
            }
        }
        for (const auto& [groupId, group] : StaticGroups) {
            Y_UNUSED(groupId);
            if (!group.Info || !group.Info->Group || !group.Info->Group->HasBridgeGroupState()) {
                continue;
            }
            const NBridgeStorage::TGroupContext context(&group.Info->Group->GetBridgeGroupState());
            for (auto& [pileId, summary] : snapshot.Piles) {
                summary.SystemGroups.Count(ObserveBridgeStorageCopy(context, *BridgeInfo->GetPile(pileId),
                                                                    syncStates.at(pileId), true)
                                               .State);
            }
        }
        snapshot.Databases = NBridgeStorage::BuildDatabaseSummaries(snapshot.Pools);
        BridgeStorageSnapshot = std::move(snapshot);
    }

    bool TBlobStorageController::SelectBridgeStoragePools(const NKikimrBridge::TStorageQuery& query,
                                                          std::set<TBoxStoragePoolId>& selected, TString& error) const {
        THashSet<TString> requested(query.GetPoolNames().begin(), query.GetPoolNames().end());
        THashMap<TString, ui32> matches;
        ui32 explicitMatches = 0;
        for (const auto& [poolId, pool] : BridgeStorageSnapshot.Pools) {
            if (query.GetFilterPools() && requested.contains(pool.Name)) {
                ++matches[pool.Name];
            }
            explicitMatches += pool.Name == query.GetStoragePool();
            if ((!query.GetFilterPools() || requested.contains(pool.Name)) &&
                (query.GetStoragePool().empty() || pool.Name == query.GetStoragePool())) {
                selected.insert(poolId);
            }
        }
        if (query.GetFilterPools()) {
            for (const auto& name : requested) {
                if (matches[name] != 1) {
                    error = "Selected database storage pools must exist and have unique names";
                    return false;
                }
            }
        }
        if (!query.GetStoragePool().empty() && explicitMatches != 1) {
            error = explicitMatches ? "Storage pool name is ambiguous" : "Storage pool not found";
            return false;
        }
        return true;
    }

    void TBlobStorageController::FillBridgeStoragePiles(const NKikimrBridge::TStorageQuery& query,
                                                        const std::set<TBoxStoragePoolId>& selected, NKikimrBridge::TStorageState& result) const {
        for (const auto& [pileId, pile] : BridgeStorageSnapshot.Piles) {
            if (!query.GetPileName().empty() && pile.Name != query.GetPileName()) {
                continue;
            }
            auto* row = result.AddPiles();
            row->SetPileName(pile.Name);
            if (!FilterStoragePools(query)) {
                pile.UserGroups.ToProto(row->MutableUserGroups());
                pile.SystemGroups.ToProto(row->MutableSystemGroups());
            } else {
                NBridgeStorage::TCounts counts;
                for (const auto& poolId : selected) {
                    counts.Add(BridgeStorageSnapshot.Pools.at(poolId).Copies.at(pileId).Groups);
                }
                counts.ToProto(row->MutableUserGroups());
            }
        }
    }

    void TBlobStorageController::FillBridgeStorageDatabases(const NKikimrBridge::TStorageQuery& query,
                                                            const NKikimrBridge::TStoragePageToken& cursor, const std::set<TBoxStoragePoolId>& selected,
                                                            NKikimrBridge::TStorageState& result) const {
        NBridgeStorage::TDatabaseSummaries filtered;
        const auto* rows = &BridgeStorageSnapshot.Databases;
        if (FilterStoragePools(query)) {
            filtered = NBridgeStorage::BuildDatabaseSummaries(BridgeStorageSnapshot.Pools, &selected);
            rows = &filtered;
        }
        auto it = cursor.HasDatabase() ? rows->upper_bound(NBridgeStorage::DatabaseCursorKey(cursor.GetDatabase())) : rows->begin();
        NBridgeStorage::TDatabaseKey last;
        if (cursor.HasDatabase()) {
            last = NBridgeStorage::DatabaseCursorKey(cursor.GetDatabase());
        }
        ui32 scanned = 0;
        for (; it != rows->end(); ++it) {
            const auto& [key, summary] = *it;
            const auto& pileName = key.PileId ? BridgeStorageSnapshot.Piles.at(key.PileId).Name : NoPileName;
            const bool matching = (query.GetPileName().empty() || !key.PileId || query.GetPileName() == pileName) &&
                                  IncludeStorageCopy(query, summary.Copy.Groups.State());
            if (scanned == MaxStorageScan || (matching && result.DatabasesSize() == StoragePageSize(query))) {
                result.SetNextPageToken(NBridgeStorage::MakePageToken(query, result.GetGeneration(), last));
                return;
            }
            ++scanned;
            last = key;
            if (!matching) {
                continue;
            }
            auto* row = result.AddDatabases();
            row->SetOwnerSchemeShardId(key.Owner.PathId.OwnerId);
            row->SetOwnerPathId(key.Owner.PathId.LocalPathId);
            for (const auto& pool : summary.StoragePools) {
                row->AddStoragePools(pool);
            }
            row->SetMode(summary.Mode);
            row->SetPileName(pileName);
            summary.Copy.Groups.ToProto(row->MutableGroups());
            row->SetState(summary.Copy.Groups.State());
            row->SetLastError(summary.Copy.LastError);
            row->SetLastErrorTimestampUs(summary.Copy.LastErrorTimestamp.MicroSeconds());
        }
    }

    void TBlobStorageController::FillBridgeStorageGroups(const NKikimrBridge::TStorageQuery& query,
                                                         const NKikimrBridge::TStoragePageToken& cursor, const std::set<TBoxStoragePoolId>& selected,
                                                         NKikimrBridge::TStorageState& result) {
        result.SetCollectedAtUs(TActivationContext::Now().MicroSeconds()); // these details observe live state
        const auto& previous = cursor.GetGroup();
        const TBoxStoragePoolId previousPool(previous.GetBoxId(), previous.GetStoragePoolId());
        auto last = previous;
        auto continuation = [&] {
            result.SetNextPageToken(NBridgeStorage::MakePageToken(query, result.GetGeneration(), last));
        };
        auto consumedGroup = [&](TBoxStoragePoolId poolId, TGroupId groupId) {
            last.SetBoxId(std::get<0>(poolId));
            last.SetStoragePoolId(std::get<1>(poolId));
            last.SetGroupId(groupId.GetRawId());
            last.ClearLastPileId();
        };
        std::map<TBridgePileId, const NKikimrBridge::TClusterStateDetails::TPileSyncState*> syncStates;
        for (const auto& [pileId, pile] : BridgeStorageSnapshot.Piles) {
            Y_UNUSED(pile);
            syncStates[pileId] = FindPileSyncState(pileId);
        }
        ui32 scanned = 0;
        auto poolIt = cursor.HasGroup() ? selected.lower_bound(previousPool) : selected.begin();
        for (; poolIt != selected.end(); ++poolIt) {
            const auto poolId = *poolIt;
            const auto livePool = StoragePools.find(poolId);
            if (livePool == StoragePools.end()) {
                continue;
            }
            const auto& pool = livePool->second;
            if (BridgeStorageSnapshot.Pools.at(poolId).Name != pool.Name) {
                result.ClearGroups();
                result.SetUnavailableReason("Storage pool names changed; storage diagnostics are refreshing");
                return;
            }
            const bool resumePool = cursor.HasGroup() && poolId == previousPool;
            const auto firstGroup = resumePool ? TGroupId::FromValue(previous.GetGroupId()) : TGroupId::Zero();
            auto it = resumePool && !previous.HasLastPileId() ? StoragePoolGroups.upper_bound({poolId, firstGroup}) : StoragePoolGroups.lower_bound({poolId, firstGroup});
            for (; it != StoragePoolGroups.end() && it->first == poolId; ++it) {
                const auto* group = FindGroup(it->second);
                if (!group || (group->BridgePileId && !group->BridgeGroupInfo) || group->VirtualGroupState) {
                    if (scanned == MaxStorageScan) {
                        continuation();
                        return;
                    }
                    ++scanned; // skipped physical parts also consume budget and advance the whole-group cursor
                    consumedGroup(poolId, it->second);
                    continue;
                }
                const NBridgeStorage::TGroupContext context(group->BridgeGroupInfo ? &group->BridgeGroupInfo->GetBridgeGroupState() : nullptr);
                for (const auto& [pileId, pile] : BridgeStorageSnapshot.Piles) {
                    if (!pool.BridgeMode && (context.AuthorityPileId ? context.AuthorityPileId != pileId : pileId != BridgeStorageSnapshot.Piles.begin()->first)) {
                        continue; // ordinary storage has one retained copy, or no implied per-pile placement
                    }
                    if (resumePool && group->ID.GetRawId() == previous.GetGroupId() && previous.HasLastPileId() &&
                        pileId.GetLocalDb() <= previous.GetLastPileId()) {
                        continue;
                    }
                    if (scanned == MaxStorageScan) {
                        continuation();
                        return;
                    }
                    const auto copy = ObserveBridgeStorageCopy(context, *BridgeInfo->GetPile(pileId),
                                                               syncStates.at(pileId), pool.BridgeMode);
                    const bool unplacedOrdinary = !pool.BridgeMode && !context.AuthorityPileId;
                    const TString& pileName = unplacedOrdinary ? NoPileName : pile.Name;
                    const bool matching = (query.GetPileName().empty() || unplacedOrdinary || query.GetPileName() == pileName) &&
                                          IncludeStorageCopy(query, copy.State);
                    if (matching && result.GroupsSize() == StoragePageSize(query)) {
                        continuation();
                        return;
                    }
                    ++scanned;
                    consumedGroup(poolId, group->ID);
                    last.SetLastPileId(pileId.GetLocalDb());
                    if (matching) {
                        auto row = DescribeBridgeStorageGroup(group->ID, pool.Name, pileName, copy);
                        if (!pool.BridgeMode && !context.State) {
                            row.SetPartGroupId(group->ID.GetRawId());
                        }
                        result.AddGroups()->Swap(&row);
                    }
                }
                consumedGroup(poolId, group->ID);
            }
        }
    }

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerQueryBridgeStorage::TPtr& ev) {
        const auto& query = ev->Get()->Record;
        auto response = std::make_unique<TEvBlobStorage::TEvControllerQueryBridgeStorageResult>();
        auto& result = response->Record;
        auto reply = [&] { Send(ev->Sender, response.release(), 0, ev->Cookie); };
        if (!StorageConfig || !BridgeInfo || BridgeInfo->IsImplicit || !BridgeStorageSnapshot.CollectedAt ||
            BridgeStorageSnapshot.Generation != StorageConfig->GetClusterState().GetGeneration()) {
            result.SetUnavailableReason("Storage diagnostics are refreshing for the current cluster-state generation");
            return reply();
        }
        result.SetGeneration(BridgeStorageSnapshot.Generation);
        result.SetCollectedAtUs(BridgeStorageSnapshot.CollectedAt.MicroSeconds());
        if (query.GetView() != NKikimrBridge::TStorageQuery::PILES &&
            query.GetView() != NKikimrBridge::TStorageQuery::DATABASES &&
            query.GetView() != NKikimrBridge::TStorageQuery::GROUPS) {
            result.SetUnavailableReason("Unknown storage diagnostic view");
            return reply();
        }
        if (query.GetPageSize() > MaxStorageScan) {
            result.SetUnavailableReason("Storage page_size must not exceed 10000");
            return reply();
        }
        NKikimrBridge::TStoragePageToken cursor;
        if (!NBridgeStorage::ParsePageToken(query, result.GetGeneration(), cursor) ||
            (cursor.HasDatabase() && cursor.GetDatabase().GetPileId() && !BridgeStorageSnapshot.Piles.contains(TBridgePileId::FromLocalDb(cursor.GetDatabase().GetPileId()))) ||
            (cursor.HasGroup() && cursor.GetGroup().HasLastPileId() && !BridgeStorageSnapshot.Piles.contains(TBridgePileId::FromLocalDb(cursor.GetGroup().GetLastPileId())))) {
            result.SetUnavailableReason("Invalid page token, changed filters, or changed cluster-state generation; restart pagination");
            return reply();
        }
        if (!query.GetPileName().empty() && std::none_of(BridgeStorageSnapshot.Piles.begin(), BridgeStorageSnapshot.Piles.end(),
                                                         [&](const auto& pile) { return pile.second.Name == query.GetPileName(); })) {
            result.SetUnavailableReason("Pile not found");
            return reply();
        }
        std::set<TBoxStoragePoolId> selected;
        TString error;
        if (FilterStoragePools(query) && !SelectBridgeStoragePools(query, selected, error)) {
            result.SetUnavailableReason(error);
            return reply();
        }
        switch (query.GetView()) {
            case NKikimrBridge::TStorageQuery::PILES:
                FillBridgeStoragePiles(query, selected, result);
                break;
            case NKikimrBridge::TStorageQuery::DATABASES:
                FillBridgeStorageDatabases(query, cursor, selected, result);
                break;
            case NKikimrBridge::TStorageQuery::GROUPS:
                if (!FilterStoragePools(query)) {
                    result.SetUnavailableReason("Group diagnostics require a database or storage pool filter");
                } else {
                    FillBridgeStorageGroups(query, cursor, selected, result);
                }
                break;
            default:
                Y_ABORT(); // checked before query dispatch
        }
        reply();
    }

} // namespace NKikimr::NBsController
