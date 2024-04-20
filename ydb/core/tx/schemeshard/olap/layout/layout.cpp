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
    TTableIdsGroup emptyGroup;
    TShardIdsGroup shardIdsGroup;
    for (const auto& tabletId : tabletIds) {
        shardIdsGroup.AddId(tabletId);
    }
    return TColumnTablesLayout({ TTablesGroup(std::move(emptyGroup), std::move(shardIdsGroup)) });
}

TColumnTablesLayout::TColumnTablesLayout(std::vector<TTablesGroup>&& groups)
    : Groups(std::move(groups))
{
    AFL_VERIFY(std::is_sorted(Groups.begin(), Groups.end()));
}

bool TColumnTablesLayout::TTablesGroup::TryMerge(const TTablesGroup& item) {
    if (GetTableIds() == item.GetTableIds()) {
        for (auto&& i : item.ShardIds) {
            AFL_VERIFY(ShardIds.AddId(i));
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

}
