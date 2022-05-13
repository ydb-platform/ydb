#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    using NTabletFlatExecutor::TTabletExecutedFlat;

    class TBlobDepot
        : public TActor<TBlobDepot>
        , public TTabletExecutedFlat
    {
    public:
        TBlobDepot(TActorId tablet, TTabletStorageInfo *info)
            : TActor(&TThis::StateFunc)
            , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        {}

        void OnActivateExecutor(const TActorContext&) override
        {}

        void OnDetach(const TActorContext&) override {
            // TODO: what does this callback mean
            PassAway();
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& /*ev*/, const TActorContext&) override {
            PassAway();
        }

        STFUNC(StateFunc) {
            HandleDefaultEvents(ev, ctx);
        }
    };

} // NKikimr::NBlobDepot
