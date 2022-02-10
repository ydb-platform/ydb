#pragma once

#include "defs.h"
#include "vdisk_context.h"
#include "vdisk_pdiskctx.h"

namespace NKikimr {

    struct THullLogCtx {
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        TActorId SkeletonId;
        TActorId SyncLogId;
        TActorId HugeKeeperId;

        THullLogCtx(
                const TIntrusivePtr<TVDiskContext> &vctx,
                const TPDiskCtxPtr &pdiskCtx,
                TActorId skeletonId,
                TActorId syncLogId,
                TActorId hugeKeeperId)
            : VCtx(vctx)
            , PDiskCtx(pdiskCtx)
            , SkeletonId(skeletonId)
            , SyncLogId(syncLogId)
            , HugeKeeperId(hugeKeeperId)
        {}
    };
} // NKikimr

