#include "blob_sack.h"
#include "blob_sack_tablet.h"

namespace NKikimr::NBlobSack {

    IActor *CreateBlobSack(const TActorId& tablet, TTabletStorageInfo *info) {
        return new TBlobSack(tablet, info);
    }

} // NKikimr::NBlobSack
