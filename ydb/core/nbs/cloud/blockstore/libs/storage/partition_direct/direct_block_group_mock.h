#pragma once

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TOracleMock: public IOracle
{
    TDuration WriteHedgingDelay;
    TDuration WriteRequestTimeout;
    TDuration PBufferReplyTimeout;
    EWriteMode WriteMode = EWriteMode::DirectPBuffersFilling;

    void OnRequestStarted(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) override;
    void OnRequestSucceeded(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now,
        TDuration executionTime) override;
    void OnRequestFailed(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) override;

    [[nodiscard]] THostIndex SelectBestPBufferHost(
        std::span<const THostIndex> hostIndexes,
        EOperation operation) const override;

    [[nodiscard]] TDuration GetWriteHedgingDelay() const override;
    [[nodiscard]] TDuration GetWriteRequestTimeout() const override;
    [[nodiscard]] TDuration GetPBufferReplyTimeout() const override;
    [[nodiscard]] EWriteMode GetWriteMode() const override;

    [[nodiscard]] TString Dump() const override;
};

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
    using TWriteBlocksToManyPBuffersHandler = std::function<void(
        ui32 vChunkIndex,
        THostIndex coordinatorHostIndex,
        TVector<THostIndex> hostIndexes,
        ui64 lsn,
        TBlockRange64 range,
        TDuration replyTimeout,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId,
        TWriteBlocksToManyPBuffersCallback callback)>;
    using TSyncWithPBufferHandler =
        std::function<NThreading::TFuture<TDBGFlushResponse>(
            ui32 vChunkIndex,
            THostIndex pbufferHostIndex,
            THostIndex ddiskHostIndex,
            const TVector<TPBufferSegment>& segments,
            const NWilson::TTraceId& traceId)>;
    using TBatchEraseFromPBufferHandler =
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

    using TDBGDumpHandler =
        std::function<NThreading::TFuture<TDBGDumpResponse>()>;

    TExecutorPtr Executor;
    TOracleMock Oracle;
    TScheduleHandler ScheduleHandler;
    TReadBlocksFromDDiskHandler ReadBlocksFromDDiskHandler;
    TReadBlocksFromPBufferHandler ReadBlocksFromPBufferHandler;
    TWriteBlocksToDDiskHandler WriteBlocksToDDiskHandler;
    TWriteBlocksToPBufferHandler WriteBlocksToPBufferHandler;
    TWriteBlocksToManyPBuffersHandler WriteBlocksToManyPBuffersHandler;
    TSyncWithPBufferHandler SyncWithPBufferHandler;
    TBatchEraseFromPBufferHandler BatchEraseFromPBufferHandler;
    TDBGRestoreHandler RestoreDBGPBuffersHandler;
    TListPBuffersHandler ListPBuffersHandler;
    TDBGDumpHandler DumpHandler;

    TVector<TVChunkWeakPtr> VChunks;

    TDirectBlockGroupMock();

    void Register(TVChunkWeakPtr vChunk) override;

    TExecutorPtr GetExecutor() override;
    IOraclePtr GetOracle() override;

    void Schedule(TDuration delay, TCallback callback) override;

    std::shared_ptr<NWilson::TSpan> CreateChildSpan(
        const NWilson::TTraceId& traceId,
        TStringBuf name) override;

    void Run(IPartitionDirectService* service) override;

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

    void WriteBlocksToManyPBuffers(
        ui32 vChunkIndex,
        THostIndex coordinatorHostIndex,
        TVector<THostIndex> hostIndexes,
        ui64 lsn,
        TBlockRange64 range,
        TDuration replyTimeout,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId,
        TWriteBlocksToManyPBuffersCallback callback) override;

    NThreading::TFuture<TDBGFlushResponse> SyncWithPBuffer(
        ui32 vChunkIndex,
        THostIndex pbufferHostIndex,   // source host
        THostIndex ddiskHostIndex,     // destination host
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGEraseResponse> BatchEraseFromPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) override;

    void BarrierEraseFromPBuffer(ui64 lsn) override;

    NThreading::TFuture<std::optional<ui64>>
    GatherSafeBarrierForErase() override;

    NThreading::TFuture<TDBGRestoreResponse> RestoreDBGPBuffers(
        ui32 vChunkIndex) override;

    NThreading::TFuture<TListPBufferResponse> ListPBuffers(
        THostIndex hostIndex) override;

    NThreading::TFuture<TDBGDumpResponse> Dump() override;
};

using TDirectBlockGroupMockPtr = std::shared_ptr<TDirectBlockGroupMock>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
