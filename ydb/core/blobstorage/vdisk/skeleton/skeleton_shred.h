#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

namespace NKikimr {

    IActor *CreateSkeletonShredActor(NPDisk::TEvShredVDisk::TPtr ev, TPDiskCtxPtr pdiskCtx, TActorId hugeKeeperId,
        TActorId defragId, TVDiskContextPtr vctx);

} // NKikimr
