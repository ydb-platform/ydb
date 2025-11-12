#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>

namespace NKikimr {

namespace NSyncLog {

////////////////////////////////////////////////////////////////////////////
// TSyncLogFirstLsnToKeep
// Global tracker for FirstLsnToKeep for SyncLog
////////////////////////////////////////////////////////////////////////////
class TSyncLogFirstLsnToKeep {
private:
    TAtomic Lsn = 0;
public:
    TSyncLogFirstLsnToKeep() = default;

    void Set(ui64 lsn) {
        AtomicSet(Lsn, lsn);
    }

    ui64 Get() const {
        return AtomicGet(Lsn);
    }
};

////////////////////////////////////////////////////////////////////////////
// TSyncLogCtx
////////////////////////////////////////////////////////////////////////////
class TSyncLogCtx : public TThrRefBase {
public:
    const TIntrusivePtr<TVDiskContext> VCtx;
    const TIntrusivePtr<TLsnMngr> LsnMngr;
    const TPDiskCtxPtr PDiskCtx;
    const TActorId LoggerID;
    const TActorId LogCutterID;

    const ui64 SyncLogMaxDiskAmount;
    const ui64 SyncLogMaxEntryPointSize;
    const ui64 SyncLogMaxMemAmount;
    const ui32 MaxResponseSize;
    std::shared_ptr<TSyncLogFirstLsnToKeep> SyncLogFirstLsnToKeep;

    NMonGroup::TSyncLogIFaceGroup IFaceMonGroup;
    NMonGroup::TSyncLogCountersGroup CountersMonGroup;
    NMonGroup::TPhantomFlagStorageGroup PhantomFlagStorageGroup;

    const bool IsReadOnlyVDisk;

    TSyncLogCtx(TIntrusivePtr<TVDiskContext> vctx,
            TIntrusivePtr<TLsnMngr> lsnMngr,
            TPDiskCtxPtr pdiskCtx,
            const TActorId &loggerId,
            const TActorId &logCutterId,
            ui64 syncLogMaxDiskAmount,
            ui64 syncLogMaxEntryPointSize,
            ui64 syncLogMaxMemAmount,
            ui32 maxResponseSize,
            std::shared_ptr<TSyncLogFirstLsnToKeep> syncLogFirstLsnToKeep,
            bool isReadOnlyVDisk)
        : VCtx(std::move(vctx))
        , LsnMngr(std::move(lsnMngr))
        , PDiskCtx(std::move(pdiskCtx))
        , LoggerID(loggerId)
        , LogCutterID(logCutterId)
        , SyncLogMaxDiskAmount(syncLogMaxDiskAmount)
        , SyncLogMaxEntryPointSize(syncLogMaxEntryPointSize)
        , SyncLogMaxMemAmount(syncLogMaxMemAmount)
        , MaxResponseSize(maxResponseSize)
        , SyncLogFirstLsnToKeep(std::move(syncLogFirstLsnToKeep))
        , IFaceMonGroup(VCtx->VDiskCounters, "subsystem", "synclog")
        , CountersMonGroup(VCtx->VDiskCounters, "subsystem", "synclogcounters")
        , PhantomFlagStorageGroup(VCtx->VDiskCounters, "subsystem", "phantomflagstorage")
        , IsReadOnlyVDisk(isReadOnlyVDisk)
    {}
};

} // namespace NSyncLog

} // namespace NKikimr
