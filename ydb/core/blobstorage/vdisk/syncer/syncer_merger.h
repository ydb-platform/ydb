#pragma once

#include "defs.h"
#include "syncer_context.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr::NSyncer {

IActor* CreateIndexMergerActor(
    TSyncerContextPtr syncerCtx,
    const std::unordered_set<TVDiskID>& vdiskIds,
    const TIntrusivePtr<TBlobStorageGroupInfo>& info);

} // namespace NKikimr::NSyncer
