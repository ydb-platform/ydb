#pragma once

#include "defs.h"
#include "syncer_context.h"
#include "blobstorage_syncer_data.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr::NSyncer {

IActor* CreateIndexMergerActor(
    TSyncerContextPtr syncerCtx,
    const TActorId& schedulerActorId,
    const std::unordered_map<TVDiskID, TPeerSyncState>& syncStates,
    const TIntrusivePtr<TBlobStorageGroupInfo>& info);

} // namespace NKikimr::NSyncer
