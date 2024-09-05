#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_outofspace.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

namespace NKikimr {

    class TVDiskContext;
    class THugeBlobCtx;
    class TPDiskCtx;
    class TBlobStorageGroupInfo;

    ////////////////////////////////////////////////////////////////////////////
    // TDefragCtx
    ////////////////////////////////////////////////////////////////////////////
    struct TDefragCtx {
        const TIntrusivePtr<TVDiskContext> VCtx;
        const TIntrusivePtr<TVDiskConfig> VCfg;
        const std::shared_ptr<THugeBlobCtx> HugeBlobCtx;
        const TPDiskCtxPtr PDiskCtx;
        const TActorId SkeletonId;
        const TActorId HugeKeeperId;
        NMonGroup::TDefragGroup DefragMonGroup;
        bool RunDefragBySchedule;

        // free up to this number of chunks in one quantum
        static constexpr ui32 MaxChunksToDefrag = 20u;

        TDefragCtx(
                const TIntrusivePtr<TVDiskContext> &vctx,
                const TIntrusivePtr<TVDiskConfig> &vconfig,
                const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
                const TPDiskCtxPtr &pdiskCtx,
                const TActorId &skeletonId,
                const TActorId &hugeKeeperId,
                bool runDefrageBySchedule);
        ~TDefragCtx();
    };

    ////////////////////////////////////////////////////////////////////////////
    // HugeHeapDefragmentationRequired
    // Making decision to start compaction
    ////////////////////////////////////////////////////////////////////////////
    bool HugeHeapDefragmentationRequired(
            const TOutOfSpaceState& oos,
            ui32 hugeCanBeFreedChunks,
            ui32 hugeTotalChunks,
            double defaultPercent);

    ////////////////////////////////////////////////////////////////////////////
    // VDISK DEFRAG ACTOR CREATOR
    // It creates an actor that represent VDisk Huge Defragmenter. It checks
    // the overhead of Huge Heap defragmentation periodically and runs
    // defragmentation proces.
    // Can defrag manually by receiving TEvBlobStorage::TEvVDefrag message.
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateDefragActor(
            const std::shared_ptr<TDefragCtx> &dCtx,
            const TIntrusivePtr<TBlobStorageGroupInfo> &info);

} // NKikimr
