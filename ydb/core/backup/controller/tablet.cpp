#include "tablet.h"
#include "tablet_impl.h"

namespace NKikimr::NBackup {

TBackupController::TBackupController(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
    Y_UNUSED(tablet, info);
}

STFUNC(TBackupController::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TBackupController::StateWork) {
    HandleDefaultEvents(ev, SelfId());
}

void TBackupController::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TBackupController::OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TBackupController::OnActivateExecutor(const TActorContext& ctx) {
    RunTxInitSchema(ctx);
}

void TBackupController::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}


void TBackupController::SwitchToWork(const TActorContext& ctx) {
    SignalTabletActive(ctx);
    Become(&TThis::StateWork);
}

void TBackupController::Reset() {

}

IActor* CreateBackupController(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TBackupController(tablet, info);
}

} // namespace NKikimr::NBackup
