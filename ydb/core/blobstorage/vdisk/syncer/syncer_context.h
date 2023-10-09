#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

namespace NKikimr {

    class TSyncerContext : public TThrRefBase {
    public:
        const TIntrusivePtr<TVDiskContext> VCtx;
        const TIntrusivePtr<TLsnMngr> LsnMngr;
        const TPDiskCtxPtr PDiskCtx;
        const TActorId SkeletonId;
        const TActorId AnubisRunnerId;
        const TActorId LoggerId;
        const TActorId LogCutterId;
        const TActorId SyncLogId;
        const TIntrusivePtr<TVDiskConfig> Config;
        NMonGroup::TSyncerGroup MonGroup;

        TSyncerContext(TIntrusivePtr<TVDiskContext> vctx,
                TIntrusivePtr<TLsnMngr> lsnMngr,
                TPDiskCtxPtr pdiskCtx,
                const TActorId &skeletonId,
                const TActorId &anubisRunnerId,
                const TActorId &loggerId,
                const TActorId &logCutterId,
                const TActorId &syncLogId,
                TIntrusivePtr<TVDiskConfig> config)
            : VCtx(std::move(vctx))
            , LsnMngr(std::move(lsnMngr))
            , PDiskCtx(std::move(pdiskCtx))
            , SkeletonId(skeletonId)
            , AnubisRunnerId(anubisRunnerId)
            , LoggerId(loggerId)
            , LogCutterId(logCutterId)
            , SyncLogId(syncLogId)
            , Config(std::move(config))
            , MonGroup(VCtx->VDiskCounters, "subsystem", "syncer")
        {
            Y_ABORT_UNLESS(VCtx && LsnMngr && PDiskCtx);
            Y_ABORT_UNLESS(SkeletonId && LoggerId && LogCutterId && SyncLogId);
        }
    };

} // NKikimr
