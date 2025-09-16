#include "reporter.h"
#include "../counters/aggregation/table_stats.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NOlap {


void TColumnShardStatisticsReporter::Bootstrap(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TColumnShardStatisticsReporter", "Bootstrapped");
    Schedule(TDuration::MilliSeconds(ReportStatisticsPeriodMs), new NColumnShard::TEvPrivate::TEvReportStatistics);
    // Schedule(TDuration::MilliSeconds(1000), new TEvReportStatistics);

    Become(&TThis::StateFunc);
}

void TColumnShardStatisticsReporter::BuildSSPipe() {
    auto ctx = ActorContext();
    NTabletPipe::TClientConfig clientConfig;
    if (!ctx.SelfID) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("", "Can't create pipe with 0 SelfID. Am I in test environment?");
        return;
    }
    StatsReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, SSId, clientConfig));

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("pipe", "built");
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

    const TActorContext& ctx = ActorContext();

    auto ev = [&]() {
        if (latestCSExecutorStats) {
            return std::move(latestCSExecutorStats);
        }
        else {
            return std::make_unique<TEvDataShard::TEvPeriodicTableStats>(TabletId, SSLocalId);
        }
    }();


    FillWhateverCan(ev);


    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("hh", ev->Record.GetTableStats().GetRowCount())("ev", ev->ToString());


    NTabletPipe::SendData(ctx, StatsReportPipe, ev.release());
        // TDuration::Seconds(SendStatsIntervalMinSeconds
    //     + RandomNumber<ui64>(SendStatsIntervalMaxSeconds - SendStatsIntervalMinSeconds))
    Schedule(TDuration::MilliSeconds(ReportStatisticsPeriodMs), new NColumnShard::TEvPrivate::TEvReportStatistics);
    // Schedule(TDuration::MilliSeconds(1000), new TEvReportStatistics);
}

void TColumnShardStatisticsReporter::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    // auto tabletId = ev->Get()->TabletId;
    auto clientId = ev->Get()->ClientId;

    // AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("Client pipe reset to ", tabletId)(" at tablet ", TabletID());

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
    latestCSExecutorStats = std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>(ev->Release().Release());
}

void TColumnShardStatisticsReporter::Handle(TEvSetSSId::TPtr& ev) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("SSId", SSId);
    SSId = ev->Get()->SSId;
    SSLocalId = ev->Get()->SSLocalId;
    BuildSSPipe();
}

}