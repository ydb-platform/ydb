#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    TStorageStatusFlags TBlobDepotAgent::GetStorageStatusFlags() const {
        return SpaceColorToStatusFlag(SpaceColor);
    }

    float TBlobDepotAgent::GetApproximateFreeSpaceShare() const {
        return ApproximateFreeSpaceShare;
    }

} // NKikimr::NBlobDepot
