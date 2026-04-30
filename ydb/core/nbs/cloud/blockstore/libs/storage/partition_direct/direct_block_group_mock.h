#pragma once

#include "direct_block_group.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroupMock: public IDirectBlockGroup
{
public:
    using TScheduleHandler =
        std::function<void(TDuration delay, TCallback callback)>;

    using TReadBlocksFromDDiskHandler =
        std::function<NThreading::TFuture<TDBGReadBlocksResponse>(
            ui32 vChunkIndex,
            THostIndex hostIndex,
            TBlockRange64 range,
            const TGuardedSgList& guardedSglist,
            const NWilson::TTraceId& traceId)>;
    using TReadBlocksFromPBufferHandler =
        std::function<NThreading::TFuture<TDBGReadBlocksResponse>(
            ui32 vChunkIndex,
            THostIndex hostIndex,
            ui64 lsn,
            TBlockRange64 range,
            const TGuardedSgList& guardedSglist,
            const NWilson::TTraceId& traceId)>;
    using TWriteBlocksToDDiskHandler =
        std::function<NThreading::TFuture<TDBGWriteBlocksResponse>(
            ui32 vChunkIndex,
            THostIndex hostIndex,
            TBlockRange64 range,
            const TGuardedSgList& guardedSglist,
            const NWilson::TTraceId& traceId)>;
    using TWriteBlocksToPBufferHandler =
        std::function<NThreading::TFuture<TDBGWriteBlocksResponse>(
            ui32 vChunkIndex,
            THostIndex hostIndex,
            ui64 lsn,
            TBlockRange64 range,
            const TGuardedSgList& guardedSglist,
            const NWilson::TTraceId& traceId)>;
    using TWriteBlocksToManyPBuffersHandler = std::function<
        NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>(
            ui32 vChunkIndex,
            TVector<THostIndex> hostIndexes,
            ui64 lsn,
            TBlockRange64 range,
            TDuration replyTimeout,
            const TGuardedSgList& guardedSglist,
            const NWilson::TTraceId& traceId)>;
    using TSyncWithPBufferHandler =
        std::function<NThreading::TFuture<TDBGFlushResponse>(
            ui32 vChunkIndex,
            THostIndex pbufferHostIndex,
            THostIndex ddiskHostIndex,
            const TVector<TPBufferSegment>& segments,
            const NWilson::TTraceId& traceId)>;
    using TEraseFromPBufferHandler =
        std::function<NThreading::TFuture<TDBGEraseResponse>(
            ui32 vChunkIndex,
            THostIndex hostIndex,
            const TVector<TPBufferSegment>& segments,
            const NWilson::TTraceId& traceId)>;
    using TDBGRestoreHandler =
        std::function<NThreading::TFuture<TDBGRestoreResponse>(
            ui32 vChunkIndex)>;
    using TListPBuffersHandler =
        std::function<NThreading::TFuture<TListPBufferResponse>(
            THostIndex hostIndex)>;

    TExecutorPtr Executor;
    TScheduleHandler ScheduleHandler;
    TReadBlocksFromDDiskHandler ReadBlocksFromDDiskHandler;
    TReadBlocksFromPBufferHandler ReadBlocksFromPBufferHandler;
    TWriteBlocksToDDiskHandler WriteBlocksToDDiskHandler;
    TWriteBlocksToPBufferHandler WriteBlocksToPBufferHandler;
    TWriteBlocksToManyPBuffersHandler WriteBlocksToManyPBuffersHandler;
    TSyncWithPBufferHandler SyncWithPBufferHandler;
    TEraseFromPBufferHandler EraseFromPBufferHandler;
    TDBGRestoreHandler RestoreDBGPBuffersHandler;
    TListPBuffersHandler ListPBuffersHandler;

    TExecutorPtr GetExecutor() override;

    void Schedule(TDuration delay, TCallback callback) override;

    std::shared_ptr<NWilson::TSpan> CreateChildSpan(
        const NWilson::TTraceId& traceId,
        TStringBuf name) override;

    void EstablishConnections() override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToDDisk(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>
    WriteBlocksToManyPBuffers(
        ui32 vChunkIndex,
        TVector<THostIndex> hostIndexes,
        ui64 lsn,
        TBlockRange64 range,
        TDuration replyTimeout,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGFlushResponse> SyncWithPBuffer(
        ui32 vChunkIndex,
        THostIndex pbufferHostIndex,   // source host
        THostIndex ddiskHostIndex,     // destination host
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGEraseResponse> EraseFromPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGRestoreResponse> RestoreDBGPBuffers(
        ui32 vChunkIndex) override;

    NThreading::TFuture<TListPBufferResponse> ListPBuffers(
        THostIndex hostIndex) override;
};

using TDirectBlockGroupMockPtr = std::shared_ptr<TDirectBlockGroupMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
