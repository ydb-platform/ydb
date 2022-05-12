#pragma once

#include "defs.h"

namespace NKikimr::NBlobSack {

    using NTabletFlatExecutor::TTabletExecutedFlat;

    class TBlobSack
        : public TActor<TBlobSack>
        , public TTabletExecutedFlat
    {
    public:
        TBlobSack(TActorId tablet, TTabletStorageInfo *info)
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

} // NKikimr::NBlobSack
