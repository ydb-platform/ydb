#pragma once

#include "defs.h"
#include "blobstorage_repl.h"
#include "blobstorage_replctx.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // CreateReplJobActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateReplJobActor(
            std::shared_ptr<TReplCtx> replCtx,
            const TActorId &parentId,
            const TLogoBlobID &startKey,
            TQueueActorMapPtr queueActorMapPtr,
            TBlobIdQueuePtr blobsToReplicatePtr,
            TBlobIdQueuePtr unreplicatedBlobsPtr,
            const std::optional<std::pair<TVDiskID, TActorId>>& donor,
            TUnreplicatedBlobRecords&& ubr,
            TMilestoneQueue&& milestoneQueue);

} // NKikimr
