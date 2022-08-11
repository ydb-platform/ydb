#include "assimilator_copier.h"

namespace NKikimr::NBlobDepot {

    using TCopierActor = TBlobDepot::TGroupAssimilatorCopierActor;

    TCopierActor::TGroupAssimilatorCopierActor(TBlobDepot *self)
        : Self(self)
        , Token(self->Token)
    {}

    void TCopierActor::Bootstrap() {
        Become(&TThis::StateFunc);
    }
    
    STATEFN(TCopierActor::StateFunc) {
        if (Token.expired()) {
            return; // BlobDepot tablet is already dead, we can't access its internals
        }

        switch (const ui32 type = ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::Poison, PassAway);

            default:
                Y_VERIFY_DEBUG(false, "unexpected event Type# %08" PRIx32, type);
                STLOG(PRI_CRIT, BLOB_DEPOT, BDT46, "unexpected event", (Id, Self->GetLogId()), (Type, type));
                break;
        }
    }

} // NKikimr::NBlobDepot
