#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    TStorageStatusFlags TBlobDepotAgent::GetStorageStatusFlags() const {
        return NKikimrBlobStorage::StatusIsValid; // FIXME: implement
    }

    float TBlobDepotAgent::GetApproximateFreeSpaceShare() const {
        return 1.0; // FIXME: implement
    }

} // NKikimr::NBlobDepot
