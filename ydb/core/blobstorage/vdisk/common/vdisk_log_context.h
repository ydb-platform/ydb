#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

namespace NKikimr {

class TLogContext : public TThrRefBase {
public:
    const TIntrusivePtr<TVDiskContext> VCtx;
    const TIntrusivePtr<TLsnMngr> LsnMngr;
    const TPDiskCtxPtr PDiskCtx;
    const TActorId LoggerId;
    const TActorId LogCutterId;

    TLogContext(TIntrusivePtr<TVDiskContext> vctx,
            TIntrusivePtr<TLsnMngr> lsnMngr,
            TPDiskCtxPtr pdiskCtx,
            const TActorId& loggerId,
            const TActorId& logCutterId);
};

} // NKikimr
