#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hulllogctx.h>


namespace NKikimr {
    struct TBalancingCtx {
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        THugeBlobCtxPtr HugeBlobCtx;
        TActorId SkeletonId;
        NMonGroup::TBalancingGroup MonGroup;

        NKikimr::THullDsSnap Snap;

        TIntrusivePtr<TVDiskConfig> VDiskCfg;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;

        THashMap<TLogoBlobID, TInstant>& LastBalancingTime;

        TBalancingCtx(
            TIntrusivePtr<TVDiskContext> vCtx,
            TPDiskCtxPtr pDiskCtx,
            THugeBlobCtxPtr hugeBlobCtx,
            TActorId skeletonId,
            NKikimr::THullDsSnap snap,
            TIntrusivePtr<TVDiskConfig> vDiskCfg,
            TIntrusivePtr<TBlobStorageGroupInfo> gInfo,
            THashMap<TLogoBlobID, TInstant>& lastBalancingTime
        )
            : VCtx(std::move(vCtx))
            , PDiskCtx(std::move(pDiskCtx))
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , SkeletonId(skeletonId)
            , MonGroup(VCtx->VDiskCounters, "subsystem", "balancing")
            , Snap(std::move(snap))
            , VDiskCfg(std::move(vDiskCfg))
            , GInfo(std::move(gInfo))
            , LastBalancingTime(lastBalancingTime)
        {
        }
    };

    struct TEvStartBalancing : TEventLocal<TEvStartBalancing, TEvBlobStorage::EvStartBalancing> {};

namespace NBalancing {

    struct TPartInfo {
        TLogoBlobID Key;
        NMatrix::TVectorType PartsMask;
        std::variant<TDiskPart, TRope> PartData;
    };

    constexpr ui32 SENDER_ID = 0;
    constexpr ui32 DELETER_ID = 1;

    constexpr static TDuration JOB_GRANULARITY = TDuration::MilliSeconds(1);
    constexpr static TDuration SEND_TIMEOUT = TDuration::Minutes(5);

} // NBalancing
} // NKikimr
