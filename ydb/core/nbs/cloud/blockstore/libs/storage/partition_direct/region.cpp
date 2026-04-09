#include "region.h"

#include "range_translate.h"
#include "ydb/core/nbs/cloud/blockstore/libs/common/constants.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t VChunkIndexFromHeaders(const TRequestHeaders& headers)
{
    return GetVChunkIndex(
        *headers.VolumeConfig,
        TranslateToRegion(*headers.VolumeConfig, headers.Range));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TRegion::TRegion(
    NActors::TActorSystem* actorSystem,
    IPartitionDirectService* partitionDirectService,
    ui32 regionIndex,
    TVector<IDirectBlockGroupPtr> directBlockGroups,
    ui32 syncRequestsBatchSize,
    TDuration writeHandoffDelay,
    TDuration traceSamplePeriod)
    : ActorSystem(actorSystem)
{
    for (size_t i = 0; i < VChunksPerRegionCount; i++) {
        const size_t vChunkIndex =
            (regionIndex * VChunksPerRegionCount) + static_cast<ui32>(i);
        const size_t dbgIndex = i % directBlockGroups.size();

        auto vChunk = std::make_shared<TVChunk>(
            ActorSystem,
            partitionDirectService,
            TVChunkConfig::Make(vChunkIndex),
            directBlockGroups[dbgIndex],
            syncRequestsBatchSize,
            writeHandoffDelay,
            traceSamplePeriod);
        vChunk->Start();
        VChunks.push_back(std::move(vChunk));
    }
}

NThreading::TFuture<TReadBlocksLocalResponse> TRegion::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    const NWilson::TTraceId& traceId)
{
    const size_t vChunkIndex = VChunkIndexFromHeaders(request->Headers);

    return VChunks[vChunkIndex]->ReadBlocksLocal(
        std::move(callContext),
        std::move(request),
        traceId);
}

NThreading::TFuture<TWriteBlocksLocalResponse> TRegion::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    EWriteMode writeMode,
    TDuration pbufferReplyTimeout,
    ui64 lsn,
    const NWilson::TTraceId& traceId)
{
    const size_t vChunkIndex = VChunkIndexFromHeaders(request->Headers);

    return VChunks[vChunkIndex]->WriteBlocksLocal(
        std::move(callContext),
        std::move(request),
        writeMode,
        pbufferReplyTimeout,
        lsn,
        traceId);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
