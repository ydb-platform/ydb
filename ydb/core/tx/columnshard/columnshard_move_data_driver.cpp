#include "columnshard_impl.h"
#include "columnshard_move_data_driver.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx) {
    if (Self->MoveDataState.Active) {
        Self->SetupMoveDataMetadata();
        Self->CheckMoveDataGate(ctx);
    }
    // Keep ticking even when idle: a reseed may arrive before the tablet restarts us.
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx) {
    if (!Self->MoveDataState.Active) {
        return;
    }
    Self->SetupMoveDataMetadata();
    Self->CheckMoveDataGate(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataReseed::TPtr& ev, const TActorContext& ctx) {
    Self->MoveDataState.HiveSender = ev->Get()->HiveSender;
    bool newGroups = false;
    for (const auto groupId : ev->Get()->Groups) {
        newGroups |= Self->MoveDataState.TargetGroups.emplace(groupId).second;
    }
    LOG_S_INFO("TMoveDataDriver: reseed newGroups=" << newGroups << " totalGroups=" << Self->MoveDataState.TargetGroups.size() << " at tablet "
                                                    << Self->TabletID());
    if (newGroups && Self->HasIndex()) {
        // Stop and rerun rather than extend in place: Refresh rebuilds the queues from scratch, and
        // the vacuum leg is not restarted because local-DB cleanup does not depend on the groups.
        auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
        index.StopMoveData();
        index.StartMoveData(Self->MoveDataState.TargetGroups);
        Self->MoveDataState.CleanupWatermark.reset();
    }
    ctx.Send(ctx.SelfID, new TEvPrivate::TEvMoveDataPoke());
}

}   // namespace NKikimr::NColumnShard
