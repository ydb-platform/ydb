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

        ui32 MinREALHugeBlobInBytes;

        TBalancingCtx(
            TIntrusivePtr<TVDiskContext> vCtx,
            TPDiskCtxPtr pDiskCtx,
            THugeBlobCtxPtr hugeBlobCtx,
            TActorId skeletonId,
            NKikimr::THullDsSnap snap,
            TIntrusivePtr<TVDiskConfig> vDiskCfg,
            TIntrusivePtr<TBlobStorageGroupInfo> gInfo,
            ui32 minREALHugeBlobInBytes
        )
            : VCtx(std::move(vCtx))
            , PDiskCtx(std::move(pDiskCtx))
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , SkeletonId(skeletonId)
            , MonGroup(VCtx->VDiskCounters, "subsystem", "balancing")
            , Snap(std::move(snap))
            , VDiskCfg(std::move(vDiskCfg))
            , GInfo(std::move(gInfo))
            , MinREALHugeBlobInBytes(minREALHugeBlobInBytes)
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

    static constexpr ui32 SENDER_ID = 0;
    static constexpr ui32 DELETER_ID = 1;

    static constexpr TDuration JOB_GRANULARITY = TDuration::MilliSeconds(1);

    static constexpr TDuration READ_BATCH_TIMEOUT = TDuration::Seconds(10);
    static constexpr TDuration SEND_BATCH_TIMEOUT = TDuration::Seconds(10);
    static constexpr TDuration REQUEST_BLOBS_ON_MAIN_BATCH_TIMEOUT = TDuration::Seconds(10);
    static constexpr TDuration DELETE_BATCH_TIMEOUT = TDuration::Seconds(10);

    static constexpr ui64 READ_TIMEOUT_TAG = 0;
    static constexpr ui64 SEND_TIMEOUT_TAG = 1;
    static constexpr ui64 REQUEST_TIMEOUT_TAG = 2;
    static constexpr ui64 DELETE_TIMEOUT_TAG = 3;

    static constexpr ui32 BATCH_SIZE = 32;

} // NBalancing
} // NKikimr
