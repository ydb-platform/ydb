#include "simple_tablet.h"

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {

TSimpleTablet::TSimpleTablet(const TActorId &tablet, TTabletStorageInfo *info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, nullptr)
{}

void TSimpleTablet::DefaultSignalTabletActive(const TActorContext&) {
    // must be empty
}

void TSimpleTablet::OnActivateExecutor(const TActorContext& ctx) {
    Become(&TThis::StateWork);
    SignalTabletActive(ctx);
}

void TSimpleTablet::OnDetach(const TActorContext &ctx) {
    return Die(ctx);
}

void TSimpleTablet::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) {
    return Die(ctx);
}

void TSimpleTablet::Handle(TEvents::TEvPing::TPtr& ev) {
    Send(ev->Sender, new TEvents::TEvPong);
}

STFUNC(TSimpleTablet::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TSimpleTablet::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvPing, Handle);
    default:
        HandleDefaultEvents(ev, SelfId());
        break;
    }
}

IActor* CreateSimpleTablet(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TSimpleTablet(tablet, info);
}

} // NKikimr
