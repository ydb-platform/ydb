#include "region.h"

#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

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
    ITraceService* traceService,
    IPartitionDirectService* partitionDirectService,
    const TDiskDescription& diskDescription,
    ui32 regionIndex,
    const TVector<IDirectBlockGroupPtr>& directBlockGroups,
    const TVChunkConfigs& vChunkConfigs,
    const TDirtyMapStateProtos& dirtyMapStates,
    ui32 syncRequestsBatchSize,
    ui64 vChunkSize)
    : ActorSystem(actorSystem)
    , DiskDescription(diskDescription)
{
    const ui64 vChunksPerRegionCount = GetVChunksPerRegion(vChunkSize);
    for (size_t i = 0; i < vChunksPerRegionCount; i++) {
        const size_t vChunkIndex = (regionIndex * vChunksPerRegionCount) + i;
        const size_t dbgIndex = vChunkIndex % directBlockGroups.size();

        const auto* persisted = vChunkConfigs.FindPtr(vChunkIndex);
        auto vChunkConfig = persisted ? *persisted
                                      : TVChunkConfig::MakeDefault(
                                            vChunkIndex,
                                            DirectBlockGroupHostCount,
                                            DefaultPrimaryCount);
        vChunkConfig.SetDBGIndex(dbgIndex);
        Y_ABORT_UNLESS(vChunkConfig.IsValid());
        Y_ABORT_UNLESS(vChunkConfig.GetVChunkIndex() == vChunkIndex);

        const auto* dirtyMapState = dirtyMapStates.FindPtr(vChunkIndex);
        auto vChunk = std::make_shared<TVChunk>(
            ActorSystem,
            traceService,
            partitionDirectService,
            DiskDescription,
            vChunkConfig,
            dirtyMapState ? *dirtyMapState : TDirtyMapStateProto(),
            directBlockGroups[dbgIndex],
            syncRequestsBatchSize,
            vChunkSize);
        VChunks.push_back(std::move(vChunk));
    }
}

void TRegion::Run()
{
    for (const auto& vChunk: VChunks) {
        vChunk->Start();
    }
}

NThreading::TFuture<void> TRegion::Stop()
{
    TVector<NThreading::TFuture<void>> stopFutures;
    for (const auto& vChunk: VChunks) {
        stopFutures.push_back(vChunk->Stop());
    }
    auto result = WaitAll(stopFutures);
    result.Subscribe(
        [weakSelf = weak_from_this()]   //
        (const NThreading::TFuture<void>& f)
        {
            Y_UNUSED(f);

            if (auto self = weakSelf.lock()) {
                self->OnVChunksStopped();
            }
        });
    return result;
}

TVChunkPtr TRegion::GetVChunk(size_t vChunkIndex) const
{
    if (vChunkIndex >= VChunks.size()) {
        return nullptr;
    }
    return VChunks[vChunkIndex];
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
    const NWilson::TTraceId& traceId)
{
    const size_t vChunkIndex = VChunkIndexFromHeaders(request->Headers);

    return VChunks[vChunkIndex]->WriteBlocksLocal(
        std::move(callContext),
        std::move(request),
        traceId);
}

void TRegion::OnVChunksStopped()
{
    VChunks.clear();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
