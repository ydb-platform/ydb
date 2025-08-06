#pragma once

#include <util/system/types.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/data_events/common/modification_type.h>

namespace NKikimr::NColumnShard {

void AddTableAccessStatsToTxStats(NKikimrQueryStats::TTxStats& stats, ui64 pathId, ui64 rows, ui64 bytes,
                                  NEvWrite::EModificationType modificationType);

} // namespace NKikimr::NColumnShard
