#include "reporter.h"
#include "../counters/aggregation/table_stats.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NOlap {


void TColumnShardStatisticsReporter::Bootstrap(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("TColumnShardStatisticsReporter", "Bootstrapped");
}

void TColumnShardStatisticsReporter::BuildSSPipe(const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    StatsReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, SSId, clientConfig));

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("pipe", "built");
}

void TColumnShardStatisticsReporter::SetSSId(ui64 sSId, const TActorContext& ctx) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("SSId", SSId);
    if (!SSId) {
        SSId = sSId;
        BuildSSPipe(ctx);
    }
}

void TColumnShardStatisticsReporter::SendPeriodicStats() {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("new", "SendPeriodicStats");
    // LOG_S_DEBUG("Send periodic stats.");

    if (!StatsReportPipe) { //TODO CHECK IF PIPE VALID
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("iurii", "debug")("no", "pipe");
        // LOG_S_DEBUG("Disabled periodic stats at tablet " << TabletID());
        return;
    }
    const auto& tabletSchemeShardLocalPathId = Owner.TablesManager.GetTabletPathIdVerified().SchemeShardLocalPathId;

    const TActorContext& ctx = ActorContext();
    const TInstant now = TAppData::TimeProvider->Now();

    if (Owner.LastStatsReport + Owner.StatsReportInterval > now) {
        LOG_S_TRACE("Skip send periodic stats: report interval = " << Owner.StatsReportInterval);
        return;
    }
    Owner.LastStatsReport = now;

    if (!StatsReportPipe) {
        LOG_S_DEBUG("Create periodic stats pipe to " << Owner.CurrentSchemeShardId << " at tablet " << Owner.TabletID());

    }

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

}