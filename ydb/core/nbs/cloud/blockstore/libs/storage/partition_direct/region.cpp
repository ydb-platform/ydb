#include "region.h"

#include "range_translate.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

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
    const TVector<IDirectBlockGroupPtr>& directBlockGroups,
    const TVChunkConfigByIndex& vChunkConfigs,
    ui32 syncRequestsBatchSize,
    ui64 vChunkSize,
    NMonitoring::TDynamicCounterPtr counters)
    : ActorSystem(actorSystem)
{
    Y_ABORT_UNLESS(vChunkSize > 0 && vChunkSize <= RegionSize);
    const ui64 vChunksPerRegionCount = RegionSize / vChunkSize;
    for (size_t i = 0; i < vChunksPerRegionCount; i++) {
        const size_t vChunkIndex = (regionIndex * vChunksPerRegionCount) + i;
        const size_t dbgIndex = vChunkIndex % directBlockGroups.size();

        NMonitoring::TDynamicCounterPtr vChunkCounters =
            counters->GetSubgroup("vchunk", ToString(vChunkIndex));

        const auto* persisted = vChunkConfigs.FindPtr(vChunkIndex);
        const auto vChunkConfig = persisted ? *persisted
                                            : TVChunkConfig::MakeDefault(
                                                  vChunkIndex,
                                                  DirectBlockGroupHostCount,
                                                  DefaultPrimaryCount);
        Y_ABORT_UNLESS(vChunkConfig.IsValid());
        Y_ABORT_UNLESS(vChunkConfig.GetVChunkIndex() == vChunkIndex);

        auto vChunk = std::make_shared<TVChunk>(
            ActorSystem,
            partitionDirectService,
            vChunkConfig,
            directBlockGroups[dbgIndex],
            syncRequestsBatchSize,
            vChunkSize,
            vChunkCounters);
        VChunks.push_back(std::move(vChunk));
    }
}

void TRegion::Run()
{
    for (const auto& vChunk: VChunks) {
        vChunk->Start();
    }
}

void TRegion::Stop()
{
    for (const auto& vChunk: VChunks) {
        vChunk->Stop();
    }
    VChunks.clear();
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
    ui64 lsn,
    const NWilson::TTraceId& traceId)
{
    const size_t vChunkIndex = VChunkIndexFromHeaders(request->Headers);

    return VChunks[vChunkIndex]->WriteBlocksLocal(
        std::move(callContext),
        std::move(request),
        lsn,
        traceId);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
