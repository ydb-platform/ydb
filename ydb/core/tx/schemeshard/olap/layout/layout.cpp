#include "layout.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NSchemeShard {

std::vector<ui64> TColumnTablesLayout::ShardIdxToTabletId(const std::vector<TShardIdx>& shards, const TSchemeShard& ss) {
    std::vector<ui64> result;
    for (const auto& shardIdx : shards) {
        auto* shardInfo = ss.ShardInfos.FindPtr(shardIdx);
        Y_ABORT_UNLESS(shardInfo, "ColumnShard not found");
        result.emplace_back(shardInfo->TabletID.GetValue());
    }
    return result;
}

TColumnTablesLayout TColumnTablesLayout::BuildTrivial(const std::vector<ui64>& tabletIds) {
    std::set<ui64> ids(tabletIds.begin(), tabletIds.end());
    return TColumnTablesLayout({ TTablesGroup(&Default<TTableIdsGroup>(), std::move(ids)) });
}

TColumnTablesLayout::TColumnTablesLayout(std::vector<TTablesGroup>&& groups)
    : Groups(std::move(groups))
{
    AFL_VERIFY(std::is_sorted(Groups.begin(), Groups.end()));
}

bool TColumnTablesLayout::TTablesGroup::TryMerge(const TTablesGroup& item) {
    if (GetTableIds() == item.GetTableIds()) {
        for (auto&& i : item.ShardIds) {
            AFL_VERIFY(ShardIds.emplace(i).second);
        }
        return true;
    } else {
        return false;
    }
}

const TColumnTablesLayout::TTableIdsGroup& TColumnTablesLayout::TTablesGroup::GetTableIds() const {
    AFL_VERIFY(TableIds);
    return *TableIds;
}

TColumnTablesLayout::TTablesGroup::TTablesGroup(const TTableIdsGroup* tableIds, std::set<ui64>&& shardIds)
    : TableIds(tableIds)
    , ShardIds(std::move(shardIds))
{
    AFL_VERIFY(TableIds);
}

}
