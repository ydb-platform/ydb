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

    NThreading::TFuture<void> EstablishConnections(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex) override;

    NThreading::TFuture<TDBGReadBlocksResponse>
    ReadBlocksLocalFromPersistentBuffer(
        ui32 vChunkIndex,
        ui8 persistentBufferIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 lsn) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksLocalFromDDisk(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksLocal(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    void SyncWithPersistentBuffer(
        TExecutorPtr executor,
        ui32 vChunkIndex,
        ui8 persistBufferIndex,
        const TVector<TSyncRequest>& syncRequests,
        NWilson::TTraceId traceId) override;

    void ErasePersistentBuffer(
        TExecutorPtr executor,
        std::shared_ptr<TEraseRequestHandler> requestHandler) override;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
