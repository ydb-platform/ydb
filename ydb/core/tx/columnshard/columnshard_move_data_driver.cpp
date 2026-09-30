#include "columnshard_impl.h"
#include "columnshard_move_data_driver.h"

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

}   // namespace NKikimr::NColumnShard
