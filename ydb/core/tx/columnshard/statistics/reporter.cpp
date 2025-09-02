#include "reporter.h"
#include "../counters/aggregation/table_stats.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NOlap {


void TColumnShardStatisticsReporter::Bootstrap(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TColumnShardStatisticsReporter", "Bootstrapped");
    // Schedule(TDuration::MilliSeconds(ReportBaseStatisticsPeriodMs), new TEvReportBaseStatistics);
    // Schedule(TDuration::MilliSeconds(ReportExecutorStatisticsPeriodMs), new TEvReportExecutorStatistics);
    Schedule(TDuration::MilliSeconds(1000), new TEvReportBaseStatistics);
    Schedule(TDuration::MilliSeconds(1000), new TEvReportExecutorStatistics);

    Become(&TThis::StateFunc);
}

void TColumnShardStatisticsReporter::BuildSSPipe(const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    StatsReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, SSId, clientConfig));

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("pipe", "built");
}

void TColumnShardStatisticsReporter::SendPeriodicStats() {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("new", "SendPeriodicStats");

    if (!StatsReportPipe) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("no", "pipe");
        return;
    }
    const auto& tabletSchemeShardLocalPathId = Owner.TablesManager.GetTabletPathIdVerified().SchemeShardLocalPathId;

    const TActorContext& ctx = ActorContext();

    auto ev = std::make_unique<TEvDataShard::TEvPeriodicTableStats>(Owner.TabletID(), tabletSchemeShardLocalPathId.GetRawValue());

    Owner.FillOlapStats(ctx, ev);
    Owner.FillColumnTableStats(ctx, ev);

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("ev", ev->ToString());

    NTabletPipe::SendData(ctx, StatsReportPipe, ev.release());
}

void TColumnShardStatisticsReporter::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    // auto tabletId = ev->Get()->TabletId;
    auto clientId = ev->Get()->ClientId;

    // AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("Client pipe reset to ", tabletId)(" at tablet ", TabletID());

    AFL_VERIFY(clientId == StatsReportPipe);
    StatsReportPipe = {};
    BuildSSPipe(ctx);
    return;
}

void TColumnShardStatisticsReporter::Handle(TEvTabletPipe::TEvClientConnected::TPtr&, const TActorContext&) {
    // AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("Client pipe reset to ", tabletId)(" at tablet ", TabletID());
    return;
}

void TColumnShardStatisticsReporter::ReportBaseStatistics() {
}

void TColumnShardStatisticsReporter::ReportExecutorStatistics() {
}

void TColumnShardStatisticsReporter::Handle(TEvReportBaseStatistics::TPtr&, const NActors::TActorContext&) {
    SendPeriodicStats();
    // TDuration::Seconds(SendStatsIntervalMinSeconds
    //     + RandomNumber<ui64>(SendStatsIntervalMaxSeconds - SendStatsIntervalMinSeconds))
    // Schedule(TDuration::MilliSeconds(ReportBaseStatisticsPeriodMs), new TEvReportBaseStatistics);
    Schedule(TDuration::MilliSeconds(1000), new TEvReportBaseStatistics);
}
void TColumnShardStatisticsReporter::Handle(TEvReportExecutorStatistics::TPtr&, const NActors::TActorContext&) {
    SendPeriodicStats();
    // Schedule(TDuration::MilliSeconds(ReportExecutorStatisticsPeriodMs), new TEvReportExecutorStatistics);
    Schedule(TDuration::MilliSeconds(1000), new TEvReportExecutorStatistics);
}

void TColumnShardStatisticsReporter::Handle(TEvSetSSId::TPtr& ev, const NActors::TActorContext& ctx) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("SSId", SSId);
    if (!SSId) {
        SSId = ev->Get()->SSId;
        BuildSSPipe(ctx);
    }
}

}