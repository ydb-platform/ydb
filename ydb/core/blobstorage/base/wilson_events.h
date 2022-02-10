#pragma once


#include "blobstorage_vdiskid.h"
#include <ydb/library/wilson/wilson_event.h>

#define WILSON_TRACE_FROM_ACTOR(CTX, ACTOR, TRACE_ID, EVENT_TYPE, ...) \
    WILSON_TRACE(CTX, TRACE_ID, EVENT_TYPE, \
        ActivityType = static_cast<NKikimrServices::TActivity::EType>((ACTOR).GetActivityType()), \
        ActorId = (ACTOR).SelfId(), \
        ##__VA_ARGS__);

#define DECLARE_ACTOR_EVENT(EVENT_TYPE, ...) \
    DECLARE_WILSON_EVENT(EVENT_TYPE, \
        (::NKikimrServices::TActivity::EType, ActivityType), \
        (::NActors::TActorId, ActorId), \ 
        ##__VA_ARGS__ \
    )

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // DSPROXY
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    DECLARE_ACTOR_EVENT(MultiGetReceived);
    DECLARE_ACTOR_EVENT(EvGetSent);
    DECLARE_ACTOR_EVENT(EvGetReceived);
    DECLARE_ACTOR_EVENT(EvVGetSent);
    DECLARE_ACTOR_EVENT(EvVGetReceived);
    DECLARE_ACTOR_EVENT(EvVGetResultSent);
    DECLARE_ACTOR_EVENT(EvVGetResultReceived, (::NWilson::TTraceId, MergedNode));
    DECLARE_ACTOR_EVENT(EvGetResultSent, (::NKikimrProto::EReplyStatus, ReplyStatus), (ui32, ResponseSize));
    DECLARE_ACTOR_EVENT(EvGetResultReceived, (::NWilson::TTraceId, MergedNode));
    DECLARE_ACTOR_EVENT(MultiGetResultSent);

    DECLARE_ACTOR_EVENT(RangeGetReceived);
    DECLARE_ACTOR_EVENT(RangeGetResultSent, (::NKikimrProto::EReplyStatus, ReplyStatus), (ui32, ResponseSize));

    DECLARE_ACTOR_EVENT(EvPutReceived, (ui32, Size), (TLogoBlobID, LogoBlobId));
    DECLARE_ACTOR_EVENT(EvVPutSent);
    DECLARE_ACTOR_EVENT(EvVPutResultReceived, (::NWilson::TTraceId, MergedNode));
    DECLARE_ACTOR_EVENT(EvPutResultSent, (::NKikimrProto::EReplyStatus, ReplyStatus));

    DECLARE_ACTOR_EVENT(EvDiscoverReceived, (ui32, GroupId), (TLogoBlobID, From), (TLogoBlobID, To));
    DECLARE_ACTOR_EVENT(EvDiscoverResultSent);

    DECLARE_ACTOR_EVENT(EvVGetBlockSent);
    DECLARE_ACTOR_EVENT(EvVGetBlockResultReceived, (::NWilson::TTraceId, MergedNode));

    DECLARE_ACTOR_EVENT(ReadBatcherStart);
    DECLARE_ACTOR_EVENT(ReadBatcherFinish, (::NWilson::TTraceId, MergedNode));

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // VDISK
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    DECLARE_ACTOR_EVENT(EvVPutReceived, (TVDiskID, VDiskId), (ui32, PDiskId), (ui32, VDiskSlotId));
    DECLARE_ACTOR_EVENT(EvPutIntoEmergQueue);
    DECLARE_ACTOR_EVENT(EvVPutResultSent);
    DECLARE_ACTOR_EVENT(EvVMultiPutResultSent);

    DECLARE_ACTOR_EVENT(EvChunkReadSent, (ui32, ChunkIdx), (ui32, Offset), (ui32, Size), (void*, YardCookie));

    DECLARE_ACTOR_EVENT(EvChunkReadResultReceived, (void*, YardCookie), (::NWilson::TTraceId, MergedNode));

    DECLARE_ACTOR_EVENT(EvHullWriteHugeBlobSent);
    DECLARE_ACTOR_EVENT(EvHullLogHugeBlobReceived);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // BS_QUEUE
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    DECLARE_ACTOR_EVENT(EvBlobStorageQueuePut, (ui64, InQueueWaitingItems), (ui64, InQueueWaitingBytes));
    DECLARE_ACTOR_EVENT(EvBlobStorageQueueForward, (ui64, InQueueWaitingItems), (ui64, InQueueWaitingBytes));

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // SKELETON FRONT
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    DECLARE_ACTOR_EVENT(EvSkeletonFrontEnqueue);
    DECLARE_ACTOR_EVENT(EvSkeletonFrontProceed);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // YARD
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    DECLARE_WILSON_EVENT(EvChunkReadReceived, (ui32, ChunkIdx), (ui32, Offset), (ui32, Size));
    DECLARE_WILSON_EVENT(AsyncReadScheduled, (ui64, DiskOffset), (ui32, Size));

    DECLARE_WILSON_EVENT(EvChunkWriteReceived, (ui32, ChunkIdx), (ui32, Offset), (ui32, Size));

    DECLARE_WILSON_EVENT(EvLogReceived);
    DECLARE_WILSON_EVENT(EnqueueLogWrite);
    DECLARE_WILSON_EVENT(RouteLogWrite);

    DECLARE_WILSON_EVENT(BlockPwrite, (ui64, DiskOffset), (ui32, Size));
    DECLARE_WILSON_EVENT(BlockPread, (ui64, DiskOffset), (ui32, Size));

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // LIBAIO
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    DECLARE_WILSON_EVENT(AsyncIoInQueue);
    DECLARE_WILSON_EVENT(AsyncIoFinished);

} // NKikimr

template<>
inline void Out<NKikimrServices::TActivity::EType>(IOutputStream& os, NKikimrServices::TActivity::EType status) {
    os << NKikimrServices::TActivity::EType_Name(status);
}
