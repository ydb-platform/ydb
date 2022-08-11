#pragma once

#include "defs.h"

#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilatorCopierActor : public TActorBootstrapped<TGroupAssimilatorCopierActor> {
        TBlobDepot* const Self;
        std::weak_ptr<TBlobDepot::TToken> Token;

    public:
        TGroupAssimilatorCopierActor(TBlobDepot *self);
        void Bootstrap();
        STATEFN(StateFunc);
    };

} // NKikimr::NBlobDepot
