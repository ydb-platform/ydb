#include "region.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRegion::TRegion(
    TVector<NStorage::NPartitionDirect::IDirectBlockGroupPtr> directBlockGroups)
{
    for (size_t i = 0; i < directBlockGroups.size(); i++) {
        VChunks.emplace_back(i, std::move(directBlockGroups[i]));
    }
}

NThreading::TFuture<TReadBlocksLocalResponse> TRegion::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto vChunkIndex = GetVChunkIndex(request->Range.Start);
    request->Range = TBlockRange64::WithLength(
        GetVChunkOffset(request->Range.Start),
        request->Range.Size());

    return VChunks[vChunkIndex].ReadBlocksLocal(
        std::move(callContext),
        std::move(request),
        std::move(traceId));
}

NThreading::TFuture<TWriteBlocksLocalResponse> TRegion::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto vChunkIndex = GetVChunkIndex(request->Range.Start);
    request->Range = TBlockRange64::WithLength(
        GetVChunkOffset(request->Range.Start),
        request->Range.Size());

    return VChunks[vChunkIndex].WriteBlocksLocal(
        std::move(callContext),
        std::move(request),
        std::move(traceId));
}

////////////////////////////////////////////////////////////////////////////////

size_t TRegion::GetVChunkIndex(ui64 blockIndex) const
{
    // TODO: remove hardcode
    // Basic striping by 512KB
    size_t blocksPerStripe = 512 * 1024 / 4096;
    return (blockIndex / blocksPerStripe) % VChunks.size();
}

size_t TRegion::GetVChunkOffset(ui64 blockIndex) const
{
    // TODO: remove hardcode
    // Basic striping by 512KB
    size_t blocksPerStripe = 512 * 1024 / 4096;
    auto stripeIndex = blockIndex / blocksPerStripe;
    auto stripeIndexInVChunk = stripeIndex / VChunks.size();
    auto blockIndexInStripe = blockIndex % blocksPerStripe;
    return stripeIndexInVChunk * blocksPerStripe + blockIndexInStripe;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
