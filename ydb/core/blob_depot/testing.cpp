#include "testing.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    bool IsBlobDepotActor(IActor *actor) {
        return dynamic_cast<TBlobDepot*>(actor);
    }

    void ValidateBlobDepot(IActor *actor, NTesting::TGroupOverseer& overseer) {
        if (auto *x = dynamic_cast<TBlobDepot*>(actor)) {
            x->Validate(overseer);
        } else {
            Y_FAIL();
        }
    }

    void TBlobDepot::Validate(NTesting::TGroupOverseer& overseer) const {
        Y_VERIFY(Config.HasVirtualGroupId());
        (void)overseer;
//        overseer.EnumerateBlobs(Config.GetVirtualGroupId(), [&](TLogoBlobID id, NTesting::EBlobState state) {
//            Cerr << id.ToString() << Endl;
//            (void)state;
//        });
    }

} // NKikimr::NBlobDepot
