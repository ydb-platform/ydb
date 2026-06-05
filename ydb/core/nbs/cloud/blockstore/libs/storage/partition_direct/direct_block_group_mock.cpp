#include "direct_block_group_mock.h"

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void TOracleMock::OnRequestStarted(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now)
{
    Y_UNUSED(hostIndex, operation, now);
}

void TOracleMock::OnRequestSucceeded(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now,
    TDuration executionTime)
{
    Y_UNUSED(hostIndex, operation, now, executionTime);
}

void TOracleMock::OnRequestFailed(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now)
{
    Y_UNUSED(hostIndex, operation, now);
}

THostIndex TOracleMock::SelectBestPBufferHost(
    std::span<const THostIndex> hostIndexes,
    EOperation operation) const
{
    Y_UNUSED(operation);
    return hostIndexes[0];
}

TDuration TOracleMock::GetWriteHedgingDelay() const
{
    return WriteHedgingDelay;
}

TDuration TOracleMock::GetWriteRequestTimeout() const
{
    return WriteRequestTimeout;
}

TDuration TOracleMock::GetPBufferReplyTimeout() const
{
    return PBufferReplyTimeout;
}

EWriteMode TOracleMock::GetWriteMode() const
{
    return WriteMode;
}

TString TOracleMock::Dump() const
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroupMock::TDirectBlockGroupMock()
{
    Executor = TExecutor::Create("NBS_TEST");
    Executor->Start();

    ScheduleHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set ScheduleHandler");
    };
    ReadBlocksFromDDiskHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set ReadBlocksFromDDiskHandler");
        return NThreading::TFuture<TDBGReadBlocksResponse>();
    };
    ReadBlocksFromPBufferHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set ReadBlocksFromPBufferHandler");
        return NThreading::TFuture<TDBGReadBlocksResponse>();
    };
    WriteBlocksToDDiskHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set WriteBlocksToDDiskHandler");
        return NThreading::TFuture<TDBGWriteBlocksResponse>();
    };
    WriteBlocksToPBufferHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set WriteBlocksToPBufferHandler");
        return NThreading::TFuture<TDBGWriteBlocksResponse>();
    };
    WriteBlocksToManyPBuffersHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set WriteBlocksToManyPBuffersHandler");
        return NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>();
    };
    SyncWithPBufferHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set SyncWithPBufferHandler");
        return NThreading::TFuture<TDBGFlushResponse>();
    };
    EraseFromPBufferHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set EraseFromPBufferHandler");
        return NThreading::TFuture<TDBGEraseResponse>();
    };
    RestoreDBGPBuffersHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set RestoreDBGPBuffersHandler");
        return NThreading::TFuture<TDBGRestoreResponse>();
    };
    ListPBuffersHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set ListPBuffersHandler");
        return NThreading::TFuture<TListPBufferResponse>();
    };
    DumpHandler = [](const auto&...)
    {
        Y_ABORT_UNLESS(false, "Should set DumpHandler");
        return NThreading::TFuture<TDBGDumpResponse>();
    };
}

void TDirectBlockGroupMock::Register(TVChunkWeakPtr vChunk)
{
    VChunks.push_back(std::move(vChunk));
}

TExecutorPtr TDirectBlockGroupMock::GetExecutor()
{
    return Executor;
}

IOraclePtr TDirectBlockGroupMock::GetOracle()
{
    return &Oracle;
}

void TDirectBlockGroupMock::Schedule(TDuration delay, TCallback callback)
{
    ScheduleHandler(delay, std::move(callback));
}

std::shared_ptr<NWilson::TSpan> TDirectBlockGroupMock::CreateChildSpan(
    const NWilson::TTraceId& traceId,
    TStringBuf name)
{
    Y_UNUSED(traceId);
    Y_UNUSED(name);
    return nullptr;
}

void TDirectBlockGroupMock::Run(IPartitionDirectService* service)
{
    Y_UNUSED(service);
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroupMock::ReadBlocksFromDDisk(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    return ReadBlocksFromDDiskHandler(
        vChunkIndex,
        hostIndex,
        range,
        guardedSglist,
        traceId);
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroupMock::ReadBlocksFromPBuffer(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    return ReadBlocksFromPBufferHandler(
        vChunkIndex,
        hostIndex,
        lsn,
        range,
        guardedSglist,
        traceId);
}

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroupMock::WriteBlocksToDDisk(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    return WriteBlocksToDDiskHandler(
        vChunkIndex,
        hostIndex,
        range,
        guardedSglist,
        traceId);
}

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroupMock::WriteBlocksToPBuffer(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    return WriteBlocksToPBufferHandler(
        vChunkIndex,
        hostIndex,
        lsn,
        range,
        guardedSglist,
        traceId);
}

void TDirectBlockGroupMock::WriteBlocksToManyPBuffers(
    ui32 vChunkIndex,
    THostIndex coordinatorHostIndex,
    TVector<THostIndex> hostIndexes,
    ui64 lsn,
    TBlockRange64 range,
    TDuration replyTimeout,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId,
    TWriteBlocksToManyPBuffersCallback callback)
{
    WriteBlocksToManyPBuffersHandler(
        vChunkIndex,
        coordinatorHostIndex,
        std::move(hostIndexes),
        lsn,
        range,
        replyTimeout,
        guardedSglist,
        traceId,
        std::move(callback));
}

NThreading::TFuture<TDBGFlushResponse> TDirectBlockGroupMock::SyncWithPBuffer(
    ui32 vChunkIndex,
    THostIndex pbufferHostIndex,   // source host
    THostIndex ddiskHostIndex,     // destination host
    const TVector<TPBufferSegment>& segments,
    const NWilson::TTraceId& traceId)
{
    return SyncWithPBufferHandler(
        vChunkIndex,
        pbufferHostIndex,
        ddiskHostIndex,
        segments,
        traceId);
}

NThreading::TFuture<TDBGEraseResponse> TDirectBlockGroupMock::EraseFromPBuffer(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    const TVector<TPBufferSegment>& segments,
    const NWilson::TTraceId& traceId)
{
    return EraseFromPBufferHandler(vChunkIndex, hostIndex, segments, traceId);
}

NThreading::TFuture<TDBGRestoreResponse>
TDirectBlockGroupMock::RestoreDBGPBuffers(ui32 vChunkIndex)
{
    return RestoreDBGPBuffersHandler(vChunkIndex);
}

NThreading::TFuture<TListPBufferResponse> TDirectBlockGroupMock::ListPBuffers(
    THostIndex hostIndex)
{
    return ListPBuffersHandler(hostIndex);
}

NThreading::TFuture<TDBGDumpResponse> TDirectBlockGroupMock::Dump()
{
    return DumpHandler();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
