#pragma once

#include "defs.h"

namespace NKikimr {

    struct TScrubContext : TThrRefBase {
        const TIntrusivePtr<TVDiskContext> VCtx;
        const TPDiskCtxPtr PDiskCtx;
        const TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const TActorId SkeletonId;
        const TActorId LogoBlobsLevelIndexActorId;
        const ui32 NodeId;
        const ui32 PDiskId;
        const ui32 VSlotId;
        const ui64 ScrubCookie;
        const ui64 IncarnationGuid;
        const TIntrusivePtr<TLsnMngr> LsnMngr;
        const TActorId LoggerId;
        const TActorId LogCutterId;
        const TControlWrapper EnableDeepScrubbing;

        TScrubContext(TIntrusivePtr<TVDiskContext> vctx, TPDiskCtxPtr pdiskCtx,
                TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId skeletonId, TActorId logoBlobsLevelIndexActorId,
                ui32 nodeId, ui32 pdiskId, ui32 vslotId, ui64 scrubCookie, ui64 incarnationGuid,
                const TIntrusivePtr<TLsnMngr> lsnMngr, TActorId loggerId, TActorId logCutterId,
                const TControlWrapper& enableDeepScrubbing)
            : VCtx(std::move(vctx))
            , PDiskCtx(std::move(pdiskCtx))
            , Info(std::move(info))
            , SkeletonId(skeletonId)
            , LogoBlobsLevelIndexActorId(logoBlobsLevelIndexActorId)
            , NodeId(nodeId)
            , PDiskId(pdiskId)
            , VSlotId(vslotId)
            , ScrubCookie(scrubCookie)
            , IncarnationGuid(incarnationGuid)
            , LsnMngr(std::move(lsnMngr))
            , LoggerId(loggerId)
            , LogCutterId(logCutterId)
            , EnableDeepScrubbing(enableDeepScrubbing)
        {}

        using TPtr = TIntrusivePtr<TScrubContext>;
    };

    struct TEvScrubNotify : TEventLocal<TEvScrubNotify, TEvBlobStorage::EvScrubNotify> {
        enum ECheckpoint : ui32 {
            HUGE_BLOB_SCRUBBED = 1,
            SMALL_BLOB_SCRUBBED = 2,
            INDEX_RESTORED = 4,

            ALL = HUGE_BLOB_SCRUBBED | SMALL_BLOB_SCRUBBED | INDEX_RESTORED
        };
        ui32 Checkpoints;
        bool Success;

        TEvScrubNotify(ui32 checkpoints, bool success)
            : Checkpoints(checkpoints)
            , Success(success)
        {}
    };

    struct TEvReportScrubStatus : TEventLocal<TEvReportScrubStatus, TEvBlobStorage::EvReportScrubStatus> {
        bool HasUnreadableBlobs;

        TEvReportScrubStatus(bool hasUnreadableBlobs)
            : HasUnreadableBlobs(hasUnreadableBlobs)
        {}
    };

    IActor *CreateScrubActor(TScrubContext::TPtr scrubCtx, NKikimrVDiskData::TScrubEntrypoint scrubEntrypoint,
        ui64 scrubEntrypointLsn);

} // NKikimr
