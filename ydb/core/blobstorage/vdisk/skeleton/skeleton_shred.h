#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

namespace NKikimr {

    struct TShredCtx {
        ui64 Lsn;
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        TActorId SkeletonId;
        TActorId HugeKeeperId;
        TActorId DefragId;
        TActorId SyncLogId;
    };

    using TShredCtxPtr = std::shared_ptr<TShredCtx>;

    IActor *CreateSkeletonShredActor(NPDisk::TEvShredVDisk::TPtr ev, TShredCtxPtr shredCtx);

} // NKikimr
