#include "vchunk.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TVChunk::TVChunk(
    ui32 index,
    NStorage::NPartitionDirect::IDirectBlockGroupPtr directBlockGroup)
    : Index(index)
    , DirectBlockGroup(std::move(directBlockGroup))
{
}

NThreading::TFuture<TReadBlocksLocalResponse> TVChunk::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    return DirectBlockGroup->ReadBlocksLocal(
        Index,
        std::move(callContext),
        std::move(request),
        std::move(traceId));
}

NThreading::TFuture<TWriteBlocksLocalResponse> TVChunk::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    return DirectBlockGroup->WriteBlocksLocal(
        Index,
        std::move(callContext),
        std::move(request),
        std::move(traceId));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
