#include "agent_impl.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>

namespace NKikimr::NBlobDepot {

    TStorageStatusFlags TBlobDepotAgent::GetStorageStatusFlags() const {
        return SpaceColorToStatusFlag(SpaceColor);
    }

    float TBlobDepotAgent::GetApproximateFreeSpaceShare() const {
        return ApproximateFreeSpaceShare;
    }

} // NKikimr::NBlobDepot
