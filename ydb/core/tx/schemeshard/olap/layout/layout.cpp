#include "layout.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

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

}
