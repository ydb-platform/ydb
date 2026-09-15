#include "operation_queue_timer.h"

#include "schemeshard_info_types_table.h"

namespace NKikimr::NSchemeShard {

TShardCompactionInfo::TShardCompactionInfo(const TShardIdx& id, const TPartitionStats& stats)
    : ShardIdx(id)
    , SearchHeight(stats.SearchHeight)
    , LastFullCompactionTs(stats.FullCompactionTs)
    , RowCount(stats.RowCount)
    , RowDeletes(stats.RowDeletes)
    , PartCount(stats.PartCount)
    , HasSchemaChanges(stats.HasSchemaChanges)
{}

} // namespace NKikimr::NSchemeShard
