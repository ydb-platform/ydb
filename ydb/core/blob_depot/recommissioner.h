#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupRecommissioner : public TActorBootstrapped<TGroupRecommissioner> {
        struct TEvPrivate {
        };

        std::weak_ptr<TToken> Token;
        TBlobDepot *Self;

    public:
        TGroupRecommissioner(TBlobDepot *self)
            : Token(self->Token)
            , Self(self)
        {
            Y_ABORT_UNLESS(Self->Config.HasVirtualGroupId());
        }

        void Bootstrap();
        void PassAway() override;
        STATEFN(StateFunc);
    };

} // NKikimrBlobDepot::NBlobDepot
