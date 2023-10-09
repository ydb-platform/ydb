#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>

namespace NKikimr {

IActor* CreateSkeletonVMovedPatchActor(TActorId leaderId, TOutOfSpaceStatus oosStatus,
        TEvBlobStorage::TEvVMovedPatch::TPtr &ev, TActorIDPtr skeletonFrontIDPtr,
        ::NMonitoring::TDynamicCounters::TCounterPtr multiPutResMsgsPtr,
        ui64 incarnationGuid, const TVDiskContextPtr &vCtx);

} // NKikimr
