#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <library/cpp/time_provider/time_provider.h>


namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvVDiskCutLog -- components named via EComp enum send this message to notify
    // about their recovery log lsn advance
    ////////////////////////////////////////////////////////////////////////////
    struct TEvVDiskCutLog : public TEventLocal<TEvVDiskCutLog, TEvBlobStorage::EvVDiskCutLog> {
        enum EComp {
            Start = 0,
            Hull = Start,
            SyncLog,
            Syncer,
            HugeKeeper,
            Scrub,
            Max
        };
        const EComp Component;
        const ui64 LastKeepLsn;
        const TInstant GenerationTime;

        TEvVDiskCutLog(EComp component, ui64 lastKeepLsn)
            : Component(component)
            , LastKeepLsn(lastKeepLsn)
            , GenerationTime(TAppData::TimeProvider->Now())
        {
            Y_DEBUG_ABORT_UNLESS(Start <= component && component < Max);
        }
    };

    class TVDiskContext;
    class TPDiskCtx;
    class TLsnMngr;
    struct TVDiskConfig;
    struct TLogCutterCtx {
        TIntrusivePtr<TVDiskContext> VCtx;
        TPDiskCtxPtr PDiskCtx;
        TIntrusivePtr<TLsnMngr> LsnMngr;
        TIntrusivePtr<TVDiskConfig> Config;
        TActorId LoggerId;
    };

    IActor* CreateRecoveryLogCutter(TLogCutterCtx &&logCutterCtx);

} // NKikimr
