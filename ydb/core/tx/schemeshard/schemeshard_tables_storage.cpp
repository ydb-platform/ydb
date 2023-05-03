#include "schemeshard_tables_storage.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

void TTablesStorage::OnAddObject(const TPathId& pathId, TColumnTableInfo::TPtr object) {
    const TString& tieringId = object->Description.GetTtlSettings().GetUseTiering();
    if (!!tieringId) {
        PathsByTieringId[tieringId].emplace(pathId);
    }
    for (auto&& s : object->ColumnShards) {
        TablesByShard[s].AddId(pathId);
    }
}

void TTablesStorage::OnRemoveObject(const TPathId& pathId, TColumnTableInfo::TPtr object) {
    const TString& tieringId = object->Description.GetTtlSettings().GetUseTiering();
    if (!!tieringId) {
        auto it = PathsByTieringId.find(tieringId);
        if (PathsByTieringId.end() == it) {
            return;
        }
        it->second.erase(pathId);
        if (it->second.empty()) {
            PathsByTieringId.erase(it);
        }
    }
    for (auto&& s : object->ColumnShards) {
        TablesByShard[s].RemoveId(pathId);
    }
}

const std::set<NKikimr::TPathId>& TTablesStorage::GetTablesWithTiering(const TString& tieringId) const {
    auto it = PathsByTieringId.find(tieringId);
    if (it != PathsByTieringId.end()) {
        return it->second;
    } else {
        return Default<std::set<TPathId>>();
    }
}

TColumnTableInfo::TPtr TTablesStorage::ExtractPtr(const TPathId& id) {
    auto it = Tables.find(id);
    Y_VERIFY(it != Tables.end());
    auto result = it->second;
    Tables.erase(it);
    return result;
}

TTablesStorage::TTableExtractedGuard TTablesStorage::TakeVerified(const TPathId& id) {
    return TTableExtractedGuard(*this, id, ExtractPtr(id), false);
}

TTablesStorage::TTableExtractedGuard TTablesStorage::TakeAlterVerified(const TPathId& id) {
    return TTableExtractedGuard(*this, id, ExtractPtr(id), true);
}

TTablesStorage::TTableReadGuard TTablesStorage::GetVerified(const TPathId& id) const {
    auto it = Tables.find(id);
    Y_VERIFY(it != Tables.end());
    return TTableReadGuard(it->second);
}

TTablesStorage::TTableCreatedGuard TTablesStorage::BuildNew(const TPathId& id, TColumnTableInfo::TPtr object) {
    auto it = Tables.find(id);
    Y_VERIFY(it == Tables.end());
    return TTableCreatedGuard(*this, id, object);
}

TTablesStorage::TTableCreatedGuard TTablesStorage::BuildNew(const TPathId& id) {
    auto it = Tables.find(id);
    Y_VERIFY(it == Tables.end());
    return TTableCreatedGuard(*this, id);
}

size_t TTablesStorage::Drop(const TPathId& id) {
    auto it = Tables.find(id);
    if (it == Tables.end()) {
        return 0;
    } else {
        OnRemoveObject(id, it->second);
        return Tables.erase(id);
    }
}

NKikimr::NSchemeShard::TColumnTablesLayout TTablesStorage::GetTablesLayout(const std::vector<ui64>& tabletIds) const {
    THashMap<ui64, TColumnTablesLayout::TTableIdsGroup> tablesByShard;
    for (auto&& i : tabletIds) {
        auto it = TablesByShard.find(i);
        if (it == TablesByShard.end()) {
            tablesByShard.emplace(i, TColumnTablesLayout::TTableIdsGroup());
        } else {
            tablesByShard.emplace(i, it->second);
        }
    }
    THashMap<TColumnTablesLayout::TTableIdsGroup, TColumnTablesLayout::TShardIdsGroup> shardsByTables;
    for (auto&& i : tablesByShard) {
        Y_VERIFY(shardsByTables[i.second].AddId(i.first));
    }
    std::vector<TColumnTablesLayout::TTablesGroup> groups;
    groups.reserve(shardsByTables.size());
    for (auto&& i : shardsByTables) {
        groups.emplace_back(TColumnTablesLayout::TTablesGroup(i.first, std::move(i.second)));
    }
    return TColumnTablesLayout(std::move(groups));
}

void TTablesStorage::TTableExtractedGuard::UseAlterDataVerified() {
    Y_VERIFY(Object);
    TColumnTableInfo::TPtr alterInfo = Object->AlterData;
    Y_VERIFY(alterInfo);
    alterInfo->AlterBody.Clear();
    Object = alterInfo;
}

std::vector<ui64> TColumnTablesLayout::ShardIdxToTabletId(const std::vector<TShardIdx>& shards, const TSchemeShard& ss) {
    std::vector<ui64> result;
    for (const auto& shardIdx : shards) {
        auto* shardInfo = ss.ShardInfos.FindPtr(shardIdx);
        Y_VERIFY(shardInfo, "ColumnShard not found");
        result.emplace_back(shardInfo->TabletID.GetValue());
    }
    return result;
}

TColumnTablesLayout TColumnTablesLayout::BuildTrivial(const std::vector<ui64>& tabletIds) {
    TTableIdsGroup emptyGroup;
    TShardIdsGroup shardIdsGroup;
    for (const auto& tabletId : tabletIds) {
        shardIdsGroup.AddId(tabletId);
    }
    return TColumnTablesLayout({ TTablesGroup(std::move(emptyGroup), std::move(shardIdsGroup)) });
}

bool TTablesStorage::TTableCreateOperator::InitShardingTablets(const TColumnTablesLayout& currentLayout, const ui32 shardsCount, TOlapStoreInfo::ILayoutPolicy::TPtr layoutPolicy) const {
    if (!layoutPolicy->Layout(currentLayout, shardsCount, Object->ColumnShards)) {
        ALS_ERROR(NKikimrServices::FLAT_TX_SCHEMESHARD) << "cannot layout new table with " << shardsCount << " shards";
        return false;
    }
    Object->Sharding.SetVersion(1);

    Object->Sharding.MutableColumnShards()->Clear();
    Object->Sharding.MutableColumnShards()->Reserve(Object->ColumnShards.size());
    for (ui64 columnShard : Object->ColumnShards) {
        Object->Sharding.AddColumnShards(columnShard);
    }
    Object->Sharding.ClearAdditionalColumnShards();
    return true;
}

}
