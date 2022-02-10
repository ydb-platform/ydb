#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // CreateHugeStatActor
    // Report huge blobs usage statistics based on database traversal
    ////////////////////////////////////////////////////////////////////////////
    class THugeBlobCtx;
    IActor *CreateHugeStatActor(
            TIntrusivePtr<THullCtx> hullCtx,
            const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
            const TActorId &parentId,
            THullDsSnap &&fullSnap,
            TEvBlobStorage::TEvVDbStat::TPtr &ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result);

} // NKikimr

