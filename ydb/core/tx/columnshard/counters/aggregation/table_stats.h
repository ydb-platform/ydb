#pragma once

#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/counters_manager.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NColumnShard {

class TTableStatsBuilder {
private:
    TCountersManager& Counters;
    const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor& Executor;

    void FillPortionStats(::NKikimrTableStats::TTableStats& to, const NOlap::TSimplePortionsGroupInfo& from) const {
        to.SetRowCount(from.GetRecordsCount());
        to.SetDataSize(from.GetBlobBytes());
    }

public:
    TTableStatsBuilder(TCountersManager& counters, const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor* executor)
        : Counters(counters)
        , Executor(*executor) {
    }

    void FillTableStats(TInternalPathId pathId, ::NKikimrTableStats::TTableStats& tableStats) {
        Counters.FillTableStats(pathId, tableStats);

        auto activeStats = Counters.GetPortionIndexCounters()->GetTableStats(pathId, TPortionIndexStats::TActivePortions());
        FillPortionStats(tableStats, activeStats);
    }

    void FillTotalTableStats(::NKikimrTableStats::TTableStats& tableStats) {
        Counters.FillTotalTableStats(tableStats);

        tableStats.SetInFlightTxCount(Executor.GetStats().TxInFly);
        tableStats.SetHasLoanedParts(Executor.HasLoanedParts());

        auto activeStats = Counters.GetPortionIndexCounters()->GetTotalStats(TPortionIndexStats::TActivePortions());
        FillPortionStats(tableStats, activeStats);
    }
};

} // namespace NKikimr::NColumnShard
