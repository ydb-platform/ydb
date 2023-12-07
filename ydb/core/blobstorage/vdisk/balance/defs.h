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
        const TIntrusivePtr<TLsnMngr> LsnMngr;
        std::shared_ptr<THullLogCtx> HullLogCtx;

        NKikimr::THullDsSnap Snap;

        TIntrusivePtr<TVDiskConfig> VDiskCfg;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
    };

    struct TPartInfo {
        TLogoBlobID Key;
        TIngress Ingress;
        std::variant<TDiskPart, TRope> PartData;
    };

    struct TPart {
        TLogoBlobID Key;
        TIngress Ingress;
        TRope PartData;
    };

    struct TPartOnMain {
        TLogoBlobID Key;
        TIngress Ingress;
        bool HasOnMain;
    };
} // NKikimr
