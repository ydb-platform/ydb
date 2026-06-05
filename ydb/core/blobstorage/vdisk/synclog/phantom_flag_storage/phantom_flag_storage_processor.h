#pragma once

#include "phantom_flag_storage_data.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSyncLog {

struct TPhantomFlagStorageProcessorContext {
    const TIntrusivePtr<TSyncLogCtx> SyncLogCtx;
    const NActors::TActorId SyncLogKeeperId;
    const NActors::TActorId ChunkKeeperId;
    const ui32 AppendBlockSize;
};

////////////////////////////////////////////////////////////////////////////
// PHANTOM FLAG STORAGE PROCESSOR CREATOR
// Creates the actor that writes and reads PhantomFlagStorage data
// (flags and thresholds) on disk and manages chunks via the ChunkKeeper
////////////////////////////////////////////////////////////////////////////
NActors::IActor* CreatePhantomFlagStorageProcessor(TPhantomFlagStorageData&& data,
        TPhantomFlagStorageProcessorContext&& ctx);

}  // namespace NKikimr::NSyncLog
