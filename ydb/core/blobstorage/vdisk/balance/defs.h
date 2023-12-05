#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>


namespace NKikimr {
    struct TBalancingCtx {
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        TActorId SkeletonId;
        NMonGroup::TBalancingGroup MonGroup;

        NKikimr::THullDsSnap Snap;

        TIntrusivePtr<TVDiskConfig> VDiskCfg;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
    };

    struct TPartInfo {
        TLogoBlobID Key;
        ui8 PartIdx;
        std::variant<TDiskPart, TRope> PartData;
    };

    struct TPart {
        TLogoBlobID Key;
        ui8 PartIdx;
        TRope PartData;
    };
} // NKikimr
