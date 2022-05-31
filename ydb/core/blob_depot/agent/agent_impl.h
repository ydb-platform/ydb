#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepotAgent : public TActorBootstrapped<TBlobDepotAgent> {
    public:
        TBlobDepotAgent(ui32 virtualGroupId, ui64 tabletId);
        void Bootstrap();

        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::Poison, PassAway);
        );
    };

} // NKikimr::NBlobDepot
