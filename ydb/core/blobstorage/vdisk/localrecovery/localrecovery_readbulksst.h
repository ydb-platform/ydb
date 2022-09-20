#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>

namespace NKikimr {

    class TVDiskContext;
    class TPDiskCtx;

    ////////////////////////////////////////////////////////////////////////////
    // TEvBulkSstEssenceLoaded
    ////////////////////////////////////////////////////////////////////////////
    struct TEvBulkSstEssenceLoaded
        : public TEventLocal<TEvBulkSstEssenceLoaded, TEvBlobStorage::EvBulkSstEssenceLoaded>
    {
        TAddBulkSstEssence Essence;
        ui64 RecoveryLogRecLsn = 0;

        TEvBulkSstEssenceLoaded() = default;
        TEvBulkSstEssenceLoaded(ui64 lsn)
            : RecoveryLogRecLsn(lsn)
        {}
        TEvBulkSstEssenceLoaded(TAddBulkSstEssence &&essence, ui64 lsn)
            : Essence(std::move(essence))
            , RecoveryLogRecLsn(lsn)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateBulkSstLoaderActor
    // Load SSTables according to TAddBulkSstRecoveryLogRec record
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateBulkSstLoaderActor(
            const TVDiskContextPtr &vctx,
            const TPDiskCtxPtr &pdiskCtx,
            const NKikimrVDiskData::TAddBulkSstRecoveryLogRec &proto,
            const TActorId &recipient,
            const ui64 recoveryLogRecLsn,
            bool loadLogoBlobs,
            bool loadBlocks,
            bool loadBarriers);

} // NKikimr
