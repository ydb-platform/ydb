#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hulllogctx.h>


namespace NKikimr {
    struct TBalancingCtx {
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        TActorId SkeletonId;
        NMonGroup::TBalancingGroup MonGroup;

        NKikimr::THullDsSnap Snap;

        TIntrusivePtr<TVDiskConfig> VDiskCfg;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;

        TBalancingCtx(
            TIntrusivePtr<TVDiskContext> vCtx,
            TPDiskCtxPtr pDiskCtx,
            TActorId skeletonId,
            NKikimr::THullDsSnap snap,
            TIntrusivePtr<TVDiskConfig> vDiskCfg,
            TIntrusivePtr<TBlobStorageGroupInfo> gInfo
        )
            : VCtx(std::move(vCtx))
            , PDiskCtx(std::move(pDiskCtx))
            , SkeletonId(skeletonId)
            , MonGroup(VCtx->VDiskCounters, "subsystem", "balancing")
            , Snap(std::move(snap))
            , VDiskCfg(std::move(vDiskCfg))
            , GInfo(std::move(gInfo))
        {
        }
    };

    struct TEvStartBalancing : TEventLocal<TEvStartBalancing, TEvBlobStorage::EvStartBalancing> {};
} // NKikimr
