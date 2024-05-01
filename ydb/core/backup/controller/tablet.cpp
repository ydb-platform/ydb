#include "tablet.h"
#include "tablet_impl.h"

namespace NKikimr::NBackup {

TBackupControllerTablet::TBackupControllerTablet(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
    Y_UNUSED(tablet, info);
}

STFUNC(TBackupControllerTablet::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TBackupControllerTablet::StateWork) {
    HandleDefaultEvents(ev, SelfId());
}

void TBackupControllerTablet::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TBackupControllerTablet::OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TBackupControllerTablet::OnActivateExecutor(const TActorContext& ctx) {
    RunTxInitSchema(ctx);
}

void TBackupControllerTablet::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}


void TBackupControllerTablet::SwitchToWork(const TActorContext& ctx) {
    SignalTabletActive(ctx);
    Become(&TThis::StateWork);
}

void TBackupControllerTablet::Reset() {

}

IActor* CreateBackupControllerTablet(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TBackupControllerTablet(tablet, info);
}

} // namespace NKikimr::NBackup
