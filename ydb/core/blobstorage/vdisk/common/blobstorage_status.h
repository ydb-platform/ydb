#pragma once

#include "defs.h"
#include "vdisk_context.h"
#include "vdisk_mongroups.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {

    struct TEvLocalStatus : public TEventLocal<TEvLocalStatus, TEvBlobStorage::EvLocalStatus> {
    };

    struct TEvLocalStatusResult :
            public TEventPB<TEvLocalStatusResult,
                            NKikimrBlobStorage::TEvVStatusResult,
                            TEvBlobStorage::EvLocalStatusResult> {
    };


    IActor *CreateStatusRequestHandler(
            const TIntrusivePtr<TVDiskContext> &vctx,
            const TActorId &skeletonId,
            const TActorId &syncerId,
            const TActorId &syncLogId,
            const std::shared_ptr<NMonGroup::TVDiskIFaceGroup> &ifaceMonGroup,
            const TVDiskID selfVDiskId,
            const ui64 incarnationGuid,
            const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo,
            TEvBlobStorage::TEvVStatus::TPtr &ev,
            const TActorId &notifyId,
            const TInstant &now,
            bool replDone,
            bool isReadOnly);

} // NKikimr
