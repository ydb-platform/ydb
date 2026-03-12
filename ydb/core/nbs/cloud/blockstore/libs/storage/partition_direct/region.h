#pragma once

#include "direct_block_group.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TRegion
{
public:
    TRegion(
        TVector<IDirectBlockGroupPtr> directBlockGroups,
        ui32 syncRequestsBatchSize);

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

private:
    TVector<std::shared_ptr<TVChunk>> VChunks;

    size_t GetVChunkIndex(ui64 blockIndex) const;
    size_t GetVChunkOffset(ui64 blockIndex) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
