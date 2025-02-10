#pragma once

#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/counters_manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NColumnShard {

class TTableStatsBuilder {
private:
    TCountersManager& Counters;
    const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor& Executor;
    NOlap::IColumnEngine& ColumnEngine;

    void FillPortionStats(::NKikimrTableStats::TTableStats& to, const NOlap::TColumnEngineStats::TPortionsStats& from) const {
        to.SetRowCount(from.Rows);
        ui64 bytesInBlobStorage = 0;
        for (const auto& [channel, bytes] : from.BytesByChannel) {
            auto item = to.AddChannels();
            item->SetChannel(channel);
            item->SetDataSize(bytes);
            bytesInBlobStorage += bytes;
        }
        to.SetDataSize(bytesInBlobStorage);
    }

public:
    TTableStatsBuilder(
        TCountersManager& counters, const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor* executor, NOlap::IColumnEngine& columnEngine)
        : Counters(counters)
        , Executor(*executor)
        , ColumnEngine(columnEngine) {
    }

    void FillTableStats(ui64 pathId, ::NKikimrTableStats::TTableStats& tableStats) {
        Counters.FillTableStats(pathId, tableStats);

        auto columnEngineStats = ColumnEngine.GetStats().FindPtr(pathId);
        if (columnEngineStats && *columnEngineStats) {
            auto activeStats = (*columnEngineStats)->Active();
            FillPortionStats(tableStats, activeStats);
        }
    }

    void FillTotalTableStats(::NKikimrTableStats::TTableStats& tableStats) {
        Counters.FillTotalTableStats(tableStats);

        tableStats.SetInFlightTxCount(Executor.GetStats().TxInFly);
        tableStats.SetHasLoanedParts(Executor.HasLoanedParts());

        auto activeStats = ColumnEngine.GetTotalStats().Active();
        FillPortionStats(tableStats, activeStats);
    }
};

} // namespace NKikimr::NColumnShard
