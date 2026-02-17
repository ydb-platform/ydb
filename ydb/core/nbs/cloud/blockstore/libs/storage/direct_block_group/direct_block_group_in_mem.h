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

    void EstablishConnections() override;

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
