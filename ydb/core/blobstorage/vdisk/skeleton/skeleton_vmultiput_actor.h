#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/base/batched_vec.h>
#include <ydb/core/blobstorage/base/utility.h>

namespace NKikimr {

struct TEvVMultiPutItemResult : TEventLocal<TEvVMultiPutItemResult, TEvBlobStorage::EvVMultiPutItemResult> {
    TLogoBlobID BlobId;
    ui64 ItemIdx;
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;
    bool WrittenBeyondBarrier;

    TEvVMultiPutItemResult(TLogoBlobID id, ui64 itemIdx, NKikimrProto::EReplyStatus status, TString errorReason,
            bool writtenBeyondBarrier)
        : TEventLocal()
        , BlobId(id)
        , ItemIdx(itemIdx)
        , Status(status)
        , ErrorReason(std::move(errorReason))
        , WrittenBeyondBarrier(writtenBeyondBarrier)
    {}
};

IActor* CreateSkeletonVMultiPutActor(TActorId leaderId, const TBatchedVec<NKikimrProto::EReplyStatus> &statuses,
        TOutOfSpaceStatus oosStatus, TEvBlobStorage::TEvVMultiPut::TPtr &ev,
        TActorIDPtr skeletonFrontIDPtr, ::NMonitoring::TDynamicCounters::TCounterPtr multiPutResMsgsPtr,
        ui64 incarnationGuid, TIntrusivePtr<TVDiskContext>& vCtx);

} // NKikimr
