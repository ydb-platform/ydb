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
        request->Range.Start - vChunkIndex * VChunkBlocksCount,
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
        request->Range.Start - vChunkIndex * VChunkBlocksCount,
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
    return blockIndex * 4096 / (128 * 1024 * 1024);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
