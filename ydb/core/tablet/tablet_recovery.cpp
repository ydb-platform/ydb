#include "tablet_recovery.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {

using NTabletFlatExecutor::TTabletExecutedFlat;

class TRestoreShard : public TActor<TRestoreShard>, public TTabletExecutedFlat {
public:
    TRestoreShard(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateWork)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {}

    STFUNC(StateWork) {
        HandleDefaultEvents(ev, SelfId());
    }

    void OnActivateExecutor(const TActorContext &ctx) override {
        SignalTabletActive(ctx);
    }

    virtual void OnDetach(const TActorContext &) override {
        PassAway();
    }

    virtual void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &) override {
        PassAway();
    }

    virtual void DefaultSignalTabletActive(const TActorContext &) override {}
}; // TRestoreShard

IActor* CreateRestoreShard(const TActorId &tablet, TTabletStorageInfo *info) {
    return new TRestoreShard(tablet, info);
}

} // namespace NKikimr
