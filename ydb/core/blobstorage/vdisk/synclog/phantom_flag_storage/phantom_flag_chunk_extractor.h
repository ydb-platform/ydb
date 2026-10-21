#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_context.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>

#include "phantom_flag_thresholds.h"

namespace NKikimr {

namespace NSyncLog {

////////////////////////////////////////////////////////////////////////////
// PHANTOM FLAG CHUNK EXTRACTOR CREATOR
// Creates an actor that reads a single SyncLog chunk that has been pulled
// from active rotation (ChunksToExtract), filters DoNotKeep records by
// threshold/syncedMask, and forwards the resulting phantom flags to the
// Processor. The Processor atomically persists them and retires the
// source chunk via TEvPhantomFlagStorageCommitData.RetiredChunks.
////////////////////////////////////////////////////////////////////////////
NActors::IActor* CreatePhantomFlagChunkExtractorActor(
        const TIntrusivePtr<TSyncLogCtx>& slCtx,
        const NActors::TActorId& processorId,
        TDeletedChunk chunk,
        ui32 appendBlockSize,
        std::shared_ptr<const TPhantomFlagThresholds> thresholds,
        TSyncedMask syncedMask);

} // namespace NSyncLog

} // namespace NKikimr
