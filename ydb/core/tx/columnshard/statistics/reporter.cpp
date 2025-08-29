#include "reporter.h"

void TColumnShardStatisticsReporter::Bootstrap(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TColumnShardStatisticsReporter", "Bootstrapped");
}

void TColumnShardStatisticsReporter::BuildSSPipe(const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    StatsReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, SSId, clientConfig));

}

void TColumnShardStatisticsReporter::SetSSId(ui64 sSId, const TActorContext& ctx) {
    SSId = sSId;
    BuildSSPipe(ctx);
}

void TColumnShardStatisticsReporter::SendPeriodicStats() {
    // LOG_S_DEBUG("Send periodic stats.");

    if (!StatsReportPipe) { //TODO CHECK IF PIPE VALID
        // LOG_S_DEBUG("Disabled periodic stats at tablet " << TabletID());
        return;
    }
    const auto& tabletSchemeShardLocalPathId = TablesManager.GetTabletPathIdVerified().SchemeShardLocalPathId;

    const TActorContext& ctx = ActorContext();
    const TInstant now = TAppData::TimeProvider->Now();

    if (LastStatsReport + StatsReportInterval > now) {
        LOG_S_TRACE("Skip send periodic stats: report interval = " << StatsReportInterval);
        return;
    }
    LastStatsReport = now;

    if (!StatsReportPipe) {
        LOG_S_DEBUG("Create periodic stats pipe to " << CurrentSchemeShardId << " at tablet " << TabletID());

    }

    auto ev = std::make_unique<TEvDataShard::TEvPeriodicTableStats>(TabletID(), tabletSchemeShardLocalPathId.GetRawValue());

    FillOlapStats(ctx, ev);
    FillColumnTableStats(ctx, ev);

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