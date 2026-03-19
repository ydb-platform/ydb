#include "region.h"

#include "ydb/core/nbs/cloud/blockstore/libs/common/constants.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TRegion::TRegion(
    NActors::TActorSystem* actorSystem,
    ui32 regionIndex,
    TVector<IDirectBlockGroupPtr> directBlockGroups,
    ui32 syncRequestsBatchSize,
    TDuration traceSamplePeriod)
    : ActorSystem(actorSystem)
{
    for (const auto& directBlockGroup: directBlockGroups) {
        directBlockGroup->EstablishConnections();
    }

    for (size_t i = 0; i < directBlockGroups.size(); i++) {
        const ui32 vChunkIndex =
            (regionIndex * VChunksPerRegionCount) + static_cast<ui32>(i);
        auto vChunk = std::make_shared<TVChunk>(
            ActorSystem,
            TVChunkConfig::Make(vChunkIndex),
            std::move(directBlockGroups[i]),
            syncRequestsBatchSize,
            traceSamplePeriod);
        vChunk->Start();
        VChunks.push_back(std::move(vChunk));
    }
}

NThreading::TFuture<TReadBlocksLocalResponse> TRegion::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto vChunkIndex = GetVChunkIndex(request->RegionRange.Start);
    request->VChunkRange = TBlockRange64::WithLength(
        GetVChunkOffset(request->RegionRange.Start),
        request->RegionRange.Size());

    return VChunks[vChunkIndex]->ReadBlocksLocal(
        std::move(callContext),
        std::move(request),
        std::move(traceId));
}

NThreading::TFuture<TWriteBlocksLocalResponse> TRegion::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto vChunkIndex = GetVChunkIndex(request->RegionRange.Start);
    request->VChunkRange = TBlockRange64::WithLength(
        GetVChunkOffset(request->RegionRange.Start),
        request->RegionRange.Size());

    return VChunks[vChunkIndex]->WriteBlocksLocal(
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
