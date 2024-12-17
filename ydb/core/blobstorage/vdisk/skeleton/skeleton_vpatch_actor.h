#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {

struct TEvVPatchDyingRequest : TEventLocal<
    TEvVPatchDyingRequest,
    TEvBlobStorage::EvVPatchDyingRequest>
{
    TEvVPatchDyingRequest(TLogoBlobID id)
        : PatchedBlobId(id)
    {}

    TLogoBlobID PatchedBlobId;
};

struct TEvVPatchDyingConfirm : TEventLocal<
    TEvVPatchDyingConfirm,
    TEvBlobStorage::EvVPatchDyingConfirm>
{};

struct TVPatchCtx : public TThrRefBase, TNonCopyable  {
    TQueueActorMap AsyncBlobQueues;

    TVPatchCtx() = default;
};

IActor* CreateSkeletonVPatchActor(TActorId leaderId, const TBlobStorageGroupType &gType, TEvBlobStorage::TEvVPatchStart::TPtr &ev,
        TInstant now, TActorIDPtr skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &vPatchFoundPartsMsgsPtr,
        const ::NMonitoring::TDynamicCounters::TCounterPtr &vPatchResMsgsPtr,
        const NVDiskMon::TLtcHistoPtr &getHistogram, const NVDiskMon::TLtcHistoPtr &putHistogram,
        const TIntrusivePtr<TVPatchCtx> &vPatchCtx, const TString &vDiskLogPrefix, ui64 incarnationGuid,
        const TIntrusivePtr<TVDiskContext>& vCtx);

} // NKikimr
