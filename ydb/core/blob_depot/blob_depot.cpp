#include "blob_depot.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    IActor *CreateBlobDepot(const TActorId& tablet, TTabletStorageInfo *info) {
        return new TBlobDepot(tablet, info);
    }

} // NKikimr::NBlobDepot
