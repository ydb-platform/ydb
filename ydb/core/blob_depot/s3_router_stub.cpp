#include "s3_router.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NBlobDepot {

    IActor* CreateBlobDepotS3Router(NKikimrBlobDepot::TS3BackendSettings /*settings*/) {
        Y_ABORT("S3 is not supported on Windows");
    }

} // NKikimr::NBlobDepot
