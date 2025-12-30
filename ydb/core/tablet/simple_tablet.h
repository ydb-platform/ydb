#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

class TSimpleTablet : public TActor<TSimpleTablet>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    TSimpleTablet(const TActorId &tablet, TTabletStorageInfo *info);

private:
    void DefaultSignalTabletActive(const TActorContext&) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override;
    void Handle(TEvents::TEvPing::TPtr& ev);

    STFUNC(StateInit);
    STFUNC(StateWork);
};

IActor* CreateSimpleTablet(const TActorId& tablet, TTabletStorageInfo* info);

} // NKikimr
