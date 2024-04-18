#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/base/vdisk_sync_common.h>


namespace NKikimr {

namespace NVPatchInternal {
enum EEv : ui32 {
    EvUpdateSelfVDisk = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    EvEnd,
};
}

struct TEvVPatchUpdateSelfVDisk : TEventLocal<
    TEvVPatchUpdateSelfVDisk,
    NVPatchInternal::EvUpdateSelfVDisk>
{
    TEvVPatchUpdateSelfVDisk(TVDiskID selfVDiskId)
        : SelfVDiskId(selfVDiskId)
    {}

    TVDiskID SelfVDiskId;
};

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

IActor* CreateSkeletonVPatchOrchestratorActor(
        const TIntrusivePtr<TBlobStorageGroupInfo> &gInfo,
        TActorIDPtr skeletonFrontIDPtr, 
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup,
        ui64 incarnationGuid,
        const TIntrusivePtr<TVDiskContext>& vCtx);

} // NKikimr
