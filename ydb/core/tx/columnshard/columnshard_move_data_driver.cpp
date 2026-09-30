#include "columnshard_impl.h"
#include "columnshard_move_data_driver.h"

namespace NKikimr::NColumnShard {

void TMoveDataDriver::StartAndCheckGate(const TActorContext& ctx) {
    if (!Self->MoveDataState.Active) {
        return;
    }
    if (Self->MoveDataState.TargetsChanged) {
        Self->RestartMoveDataActualizer();
    }
    Self->SetupMoveDataMetadata();
    Self->SetupMoveDataRewrites();
    Self->CheckMoveDataGate(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx) {
    if (!IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx) {
    if (!IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
}

}   // namespace NKikimr::NColumnShard
