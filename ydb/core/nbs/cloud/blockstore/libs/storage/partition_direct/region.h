#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TRegion: public std::enable_shared_from_this<TRegion>
{
public:
    TRegion(
        NActors::TActorSystem* actorSystem,
        ITraceService* traceService,
        IPartitionDirectService* partitionDirectService,
        const TDiskDescription& diskDescription,
        ui32 regionIndex,
        const TVector<IDirectBlockGroupPtr>& directBlockGroups,
        const TVChunkConfigs& vChunkConfigs,
        const TDirtyMapStateProtos& dirtyMapStates,
        ui32 syncRequestsBatchSize,
        // Volume block size, distinct from the 4 KiB DDisk integrity unit.
        ui32 blockSize,
        ui64 vChunkSize);

    void Run();

    NThreading::TFuture<void> Stop();

    [[nodiscard]] TVChunkPtr GetVChunk(size_t vChunkIndex) const;

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId);

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId);

private:
    void OnVChunksStopped();

    NActors::TActorSystem* const ActorSystem;
    const TDiskDescription DiskDescription;

    TVector<TVChunkPtr> VChunks;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
