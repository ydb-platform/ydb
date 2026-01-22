#include "vdisk_log_context.h"

namespace NKikimr {

TLogContext::TLogContext(TIntrusivePtr<TVDiskContext> vctx,
        TIntrusivePtr<TLsnMngr> lsnMngr,
        TPDiskCtxPtr pdiskCtx,
        const TActorId& loggerId,
        const TActorId& logCutterId)
    : VCtx(std::move(vctx))
    , LsnMngr(std::move(lsnMngr))
    , PDiskCtx(std::move(pdiskCtx))
    , LoggerId(loggerId)
    , LogCutterId(logCutterId)
{}

} // NKikimr
