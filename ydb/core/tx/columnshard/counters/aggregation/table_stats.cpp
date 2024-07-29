#include "table_stats.h"

namespace NKikimr::NColumnShard {

void TTableStatsBuilder::FillColumnTableStats(const TSingleColumnTableCounters& stats) {
    stats.FillStats(TableStats);
}

void TTableStatsBuilder::FillColumnTableStats(const TColumnTablesCounters& stats) {
    stats.FillStats(TableStats);
}

void TTableStatsBuilder::FillTabletStats(const TTabletCountersHandle& stats) {
    stats.FillStats(TableStats);
}

void TTableStatsBuilder::FillBackgroundControllerStats(const TBackgroundControllerCounters& stats, ui64 pathId) {
    stats.FillStats(pathId, TableStats);
}

void TTableStatsBuilder::FillBackgroundControllerStats(const TBackgroundControllerCounters& stats) {
    stats.FillTotalStats(TableStats);
}

void TTableStatsBuilder::FillExecutorStats(const NTabletFlatExecutor::NFlatExecutorSetup::IExecutor& executor) {
    TableStats.SetInFlightTxCount(executor.GetStats().TxInFly);
    TableStats.SetHasLoanedParts(executor.HasLoanedParts());
}

void TTableStatsBuilder::FillTxCompleteLag(TDuration txCompleteLag) {
    TableStats.SetTxCompleteLagMsec(txCompleteLag.MilliSeconds());
}

void TTableStatsBuilder::FillColumnEngineStats(const NOlap::TColumnEngineStats& stats) {
    auto activeStats = stats.Active(); // data stats excluding inactive and evicted
    TableStats.SetRowCount(activeStats.Rows);
    TableStats.SetDataSize(activeStats.Bytes);
    TableStats.SetPartCount(activeStats.Portions);
}

} // namespace NKikimr::NColumnShard