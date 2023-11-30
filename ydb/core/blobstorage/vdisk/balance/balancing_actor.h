#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>


namespace NKikimr {
    struct TBalancingCtx {
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        TActorId SkeletonId;

        NKikimr::THullDsSnap Snap;

        TIntrusivePtr<TVDiskConfig> VDiskCfg;
    };

    IActor* CreateBalancingActor(std::shared_ptr<TBalancingCtx> &ctx);
} // NKikimr
