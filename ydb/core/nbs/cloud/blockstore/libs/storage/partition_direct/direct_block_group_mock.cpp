#include "direct_block_group_mock.h"

using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TExecutorPtr TDirectBlockGroupMock::GetExecutor()
{
    return Executor;
}

void TDirectBlockGroupMock::Schedule(TDuration delay, TCallback callback)
{
    ScheduleHandler(delay, std::move(callback));
}

void TDirectBlockGroupMock::EstablishConnections()
{}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroupMock::ReadBlocksFromDDisk(
    ui32 vChunkIndex,
    ui8 hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    NWilson::TTraceId traceId)
{
    return ReadBlocksFromDDiskHandler(
        vChunkIndex,
        hostIndex,
        range,
        guardedSglist,
        std::move(traceId));
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroupMock::ReadBlocksFromPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    NWilson::TTraceId traceId)
{
    return ReadBlocksFromPBufferHandler(
        vChunkIndex,
        hostIndex,
        lsn,
        range,
        guardedSglist,
        std::move(traceId));
}

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroupMock::WriteBlocksToDDisk(
    ui32 vChunkIndex,
    ui8 hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    NWilson::TTraceId traceId)
{
    return WriteBlocksToDDiskHandler(
        vChunkIndex,
        hostIndex,
        range,
        guardedSglist,
        std::move(traceId));
}

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroupMock::WriteBlocksToPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    NWilson::TTraceId traceId)
{
    return WriteBlocksToPBufferHandler(
        vChunkIndex,
        hostIndex,
        lsn,
        range,
        guardedSglist,
        std::move(traceId));
}

NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>
TDirectBlockGroupMock::WriteBlocksToManyPBuffers(
    ui32 vChunkIndex,
    std::vector<ui8> hostIndexes,
    ui64 lsn,
    TBlockRange64 range,
    TDuration replyTimeout,
    const TGuardedSgList& guardedSglist,
    NWilson::TTraceId traceId)
{
    return WriteBlocksToManyPBuffersHandler(
        vChunkIndex,
        hostIndexes,
        lsn,
        range,
        replyTimeout,
        guardedSglist,
        std::move(traceId));
}

NThreading::TFuture<TDBGFlushResponse> TDirectBlockGroupMock::SyncWithPBuffer(
    ui32 vChunkIndex,
    ui8 pbufferHostIndex,   // source host
    ui8 ddiskHostIndex,     // destination host
    const TVector<TPBufferSegment>& segments,
    NWilson::TTraceId traceId)
{
    return SyncWithPBufferHandler(
        vChunkIndex,
        pbufferHostIndex,
        ddiskHostIndex,
        segments,
        std::move(traceId));
}

NThreading::TFuture<TDBGEraseResponse> TDirectBlockGroupMock::EraseFromPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    const TVector<TPBufferSegment>& segments,
    NWilson::TTraceId traceId)
{
    return EraseFromPBufferHandler(
        vChunkIndex,
        hostIndex,
        segments,
        std::move(traceId));
}

NThreading::TFuture<TDBGRestoreResponse>
TDirectBlockGroupMock::RestoreDBGPBuffers(ui32 vChunkIndex)
{
    return RestoreDBGPBuffersHandler(vChunkIndex);
}

NThreading::TFuture<TListPBufferResponse> TDirectBlockGroupMock::ListPBuffers(
    ui8 hostIndex)
{
    return ListPBuffersHandler(hostIndex);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
