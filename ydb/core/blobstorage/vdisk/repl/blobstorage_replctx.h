#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr {

    class TReplCtx {
    public:
        TIntrusivePtr<TVDiskContext> VCtx;
        TIntrusivePtr<THullCtx> HullCtx;
        TPDiskCtxPtr PDiskCtx;
        std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        ui32 MinREALHugeBlobInBytes;
        TIntrusivePtr<THullDs> HullDs;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TActorId SkeletonId;
        NMonGroup::TReplGroup MonGroup;

        // settings
        TIntrusivePtr<TVDiskConfig> VDiskCfg;
        std::shared_ptr<std::atomic_uint64_t> PDiskWriteBytes;
        const bool PausedAtStart;

        TReplCtx(
                TIntrusivePtr<TVDiskContext> vctx,
                TIntrusivePtr<THullCtx> hullCtx,
                TPDiskCtxPtr pdiskCtx,
                std::shared_ptr<THugeBlobCtx> hugeBlobCtx,
                ui32 minREALHugeBlobInBytes,
                TIntrusivePtr<THullDs> hullDs,
                TIntrusivePtr<TBlobStorageGroupInfo> info,
                const TActorId &skeletonId,
                TIntrusivePtr<TVDiskConfig> vdiskCfg,
                std::shared_ptr<std::atomic_uint64_t> pdiskWriteBytes,
                bool pausedAtStart = false)
            : VCtx(std::move(vctx))
            , HullCtx(std::move(hullCtx))
            , PDiskCtx(std::move(pdiskCtx))
            , HugeBlobCtx(std::move(hugeBlobCtx))
            , MinREALHugeBlobInBytes(minREALHugeBlobInBytes)
            , HullDs(std::move(hullDs))
            , GInfo(std::move(info))
            , SkeletonId(skeletonId)
            , MonGroup(VCtx->VDiskCounters, "subsystem", "repl")
            , VDiskCfg(std::move(vdiskCfg))
            , PDiskWriteBytes(std::move(pdiskWriteBytes))
            , PausedAtStart(pausedAtStart)
        {
            Y_ABORT_UNLESS(MinREALHugeBlobInBytes);
        }

        bool GetAddHeader() const { return !HullCtx || HullCtx->AddHeader; }
    };

} // NKikimr

