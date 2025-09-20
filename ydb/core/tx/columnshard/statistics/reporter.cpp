#include "reporter.h"
#include "../counters/aggregation/table_stats.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NOlap {


void TColumnShardStatisticsReporter::Bootstrap(const TActorContext&) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TColumnShardStatisticsReporter", "Bootstrapped");
    ScheduleStatisticsReport();
    Become(&TThis::StateFunc);
}

void TColumnShardStatisticsReporter::ScheduleStatisticsReport() {
    TDuration::Seconds(ReportStatisticsPeriodMs + RandomNumber<ui32>(JitterIntervalMS * 2) - JitterIntervalMS);
    Schedule(TDuration::MilliSeconds(100), new NColumnShard::TEvPrivate::TEvReportStatistics);
}

void TColumnShardStatisticsReporter::BuildSSPipe() {
    auto ctx = ActorContext();
    if (!ctx.SelfID || !SSId) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("", "Can't create pipe with 0 SelfID. Am I in test environment?");
        return;
    }
    StatsReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, SSId, NTabletPipe::TClientConfig()));
}

void TColumnShardStatisticsReporter::FillWhateverCan(std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev) {
    ev->Record.SetShardState(2);   // NKikimrTxDataShard.EDatashardState.Ready
    ev->Record.SetRound(StatsReportRound++);
    ev->Record.SetNodeId(ActorContext().SelfID.NodeId());
    // ev->Record.SetStartTime(StartTime().MilliSeconds());
    // if (auto* resourceMetrics = Executor()->GetResourceMetrics()) {
    //     resourceMetrics->Fill(*ev->Record.MutableTabletMetrics());
    // }
    auto tableStats = ev->Record.MutableTableStats();
    CountersManager.FillTotalTableStats(*tableStats);

    // tableStats.SetInFlightTxCount(Executor.GetStats().TxInFly);
    // tableStats.SetHasLoanedParts(Executor.HasLoanedParts());

    auto activeStats = CountersManager.GetPortionIndexCounters()->GetTotalStats(NColumnShard::TPortionIndexStats::TActivePortions());

    tableStats->SetRowCount(activeStats.GetRecordsCount());
    tableStats->SetDataSize(activeStats.GetBlobBytes());

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("HIHI", tableStats->GetRowCount());

    // auto tables = TablesManager.GetTables();

    // LOG_S_DEBUG("There are stats for " << tables.size() << " tables");
    // for (const auto& [internalPathId, table] : tables) {
    //     const auto& schemeShardLocalPathId = table.GetPathId().SchemeShardLocalPathId;
    //     auto* periodicTableStats = ev->Record.AddTables();
    //     periodicTableStats->SetDatashardId(TabletID());
    //     periodicTableStats->SetTableLocalId(schemeShardLocalPathId.GetRawValue());

    //     periodicTableStats->SetShardState(2);   // NKikimrTxDataShard.EDatashardState.Ready
    //     periodicTableStats->SetGeneration(Executor()->Generation());
    //     periodicTableStats->SetRound(StatsReportRound++);
    //     periodicTableStats->SetNodeId(ctx.SelfID.NodeId());
    //     periodicTableStats->SetStartTime(StartTime().MilliSeconds());

    //     if (auto* resourceMetrics = Executor()->GetResourceMetrics()) {
    //         resourceMetrics->Fill(*periodicTableStats->MutableTabletMetrics());
    //     }

    //     Counters.FillTableStats(pathId, tableStats);

    //     auto activeStats = Counters.GetPortionIndexCounters()->GetTableStats(pathId, TPortionIndexStats::TActivePortions());
    //     FillPortionStats(tableStats, activeStats);

    //     LOG_S_TRACE("Add stats for table, tableLocalID=" << schemeShardLocalPathId);
    // }

    // void FillPortionStats(::NKikimrTableStats::TTableStats& to, const NOlap::TSimplePortionsGroupInfo& from) const {
    //     to.SetRowCount(from.GetRecordsCount());
    //     to.SetDataSize(from.GetBlobBytes());
    // }
    // void FillTableStats(TInternalPathId pathId, ::NKikimrTableStats::TTableStats& tableStats) {
    //     Counters.FillTableStats(pathId, tableStats);

    //     auto activeStats = Counters.GetPortionIndexCounters()->GetTableStats(pathId, TPortionIndexStats::TActivePortions());
    //     FillPortionStats(tableStats, activeStats);
    // }

    // void FillTotalTableStats(::NKikimrTableStats::TTableStats& tableStats) {
    //     Counters.FillTotalTableStats(tableStats);

    //     tableStats.SetInFlightTxCount(Executor.GetStats().TxInFly);
    //     tableStats.SetHasLoanedParts(Executor.HasLoanedParts());

    //     auto activeStats = Counters.GetPortionIndexCounters()->GetTotalStats(TPortionIndexStats::TActivePortions());
    //     FillPortionStats(tableStats, activeStats);
    // }
}

void TColumnShardStatisticsReporter::SendPeriodicStats() {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("new", "SendPeriodicStats");

    if (!StatsReportPipe) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("no", "pipe");
        return;
    }

    if (latestCSExecutorStats) {

        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("HOORAU", latestCSExecutorStats->ToString());
        FillWhateverCan(latestCSExecutorStats);
        NTabletPipe::SendData(ActorContext(), StatsReportPipe, latestCSExecutorStats.release());
        return;
    }



    auto ev = std::make_unique<TEvDataShard::TEvPeriodicTableStats>(TabletId, SSLocalId);

    FillWhateverCan(ev);
     NTabletPipe::SendData(ActorContext(), StatsReportPipe, ev.release());
}

void TColumnShardStatisticsReporter::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    auto clientId = ev->Get()->ClientId;
    AFL_VERIFY(clientId == StatsReportPipe);
    StatsReportPipe = {};
    BuildSSPipe();
    return;
}

void TColumnShardStatisticsReporter::ReportBaseStatistics() {
}

void TColumnShardStatisticsReporter::ReportExecutorStatistics() {
}

void TColumnShardStatisticsReporter::Handle(TEvDataShard::TEvPeriodicTableStats::TPtr& ev) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("iurii", "debug")("test", "123");
    latestCSExecutorStats = std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>(ev->Release().Release());
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("iurii", "debug")("test", "456");
}

void TColumnShardStatisticsReporter::Handle(NColumnShard::TEvPrivate::TEvReportStatistics::TPtr&) {
    ScheduleStatisticsReport();
    SendPeriodicStats();
}

void TColumnShardStatisticsReporter::Handle(TEvSetSSId::TPtr& ev) {
    SSId = ev->Get()->SSId;
    SSLocalId = ev->Get()->SSLocalId;
    BuildSSPipe();
}

}