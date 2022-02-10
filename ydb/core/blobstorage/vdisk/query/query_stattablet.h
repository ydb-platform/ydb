#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // CreateTabletStatActor
    // Report all this VDisk knows about the requested Tablet
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateTabletStatActor(TIntrusivePtr<THullCtx> hullCtx,
                                  const TActorId &parentId,
                                  THullDsSnap &&fullSnap,
                                  TEvBlobStorage::TEvVDbStat::TPtr &ev,
                                  std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result);
} // NKikimr

