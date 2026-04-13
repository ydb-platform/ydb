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
    ui64 vChunkSize,
    TDuration writeHandoffDelay,
    TDuration traceSamplePeriod,
    NMonitoring::TDynamicCounterPtr counters)
    : ActorSystem(actorSystem)
{
    Y_ABORT_UNLESS(vChunkSize > 0 && vChunkSize <= RegionSize);
    const ui32 vChunksPerRegionCount = RegionSize / vChunkSize;
    for (size_t i = 0; i < vChunksPerRegionCount; i++) {
        const size_t vChunkIndex =
            (regionIndex * vChunksPerRegionCount) + static_cast<ui32>(i);
        const size_t dbgIndex = i % directBlockGroups.size();

        NMonitoring::TDynamicCounterPtr vChunkCounters =
            counters->GetSubgroup("vchunk", ToString(vChunkIndex));

        auto vChunk = std::make_shared<TVChunk>(
            ActorSystem,
            partitionDirectService,
            TVChunkConfig::Make(vChunkIndex),
            directBlockGroups[dbgIndex],
            syncRequestsBatchSize,
            vChunkSize,
            writeHandoffDelay,
            traceSamplePeriod,
            vChunkCounters);
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
