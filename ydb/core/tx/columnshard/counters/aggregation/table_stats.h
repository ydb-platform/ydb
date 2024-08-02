#pragma once

#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/column_tables.h>
#include <ydb/core/tx/columnshard/counters/tablet_counters.h>
#include <ydb/core/tx/columnshard/counters/background_controller.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/protos/table_stats.pb.h>

namespace NKikimr::NColumnShard {

class TTableStatsBuilder {
private:
    ::NKikimrTableStats::TTableStats& TableStats;

public:
    TTableStatsBuilder(::NKikimrTableStats::TTableStats& tableStats)
        : TableStats(tableStats) {
    }

    void FillColumnTableStats(const TSingleColumnTableCounters& stats);
    void FillColumnTableStats(const TColumnTablesCounters& stats);

    void FillTabletStats(const TTabletCountersHandle& stats);

    void FillBackgroundControllerStats(const TBackgroundControllerCounters& stats, ui64 pathId);
    void FillBackgroundControllerStats(const TBackgroundControllerCounters& stats);

    void FillScanCountersStats(const TScanCounters& stats);

    void FillExecutorStats(const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor& executor);

    void FillColumnEngineStats(const NOlap::TColumnEngineStats& stats);
};

} // namespace NKikimr::NColumnShard