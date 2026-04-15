#pragma once

#include "direct_block_group.h"

#include <ydb/library/pdisk_io/sector_map.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of DirectBlockGroup that doesn't use DDisk

class TInMemoryDirectBlockGroup
    : public IDirectBlockGroup
    , public std::enable_shared_from_this<TInMemoryDirectBlockGroup>
{
private:
    ui64 TabletId;
    TIntrusivePtr<NKikimr::NPDisk::TSectorMap> SectorMap;
    ui32 BlockSize;

    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

public:
    TInMemoryDirectBlockGroup(
        ui64 tabletId,
        ui32 generation,
        TVector<NKikimr::NBsController::TDDiskId> ddisksIds,
        TVector<NKikimr::NBsController::TDDiskId> persistentBufferDDiskIds,
        ui32 blockSize,
        ui64 blocksCount);

    ~TInMemoryDirectBlockGroup() override = default;

    TExecutorPtr GetExecutor() override;

    void EstablishConnections() override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        ui8 hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGFlushResponse> SyncWithPBuffer(
        ui32 vChunkIndex,
        ui8 pbufferHostIndex,
        ui8 ddiskHostIndex,
        const TVector<TPBufferSegment>& segments,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGEraseResponse> EraseFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        const TVector<TPBufferSegment>& segments,
        NWilson::TTraceId traceId) override;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
