#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TRegion
{
public:
    TRegion(
        NActors::TActorSystem* actorSystem,
        IPartitionDirectService* partitionDirectService,
        ui32 regionIndex,
        TVector<IDirectBlockGroupPtr> directBlockGroups,
        ui32 syncRequestsBatchSize,
        ui64 vChunkSize,
        TDuration writeHedgingDelay,
        TDuration writeRequestTimeout,
        TDuration traceSamplePeriod,
        NMonitoring::TDynamicCounterPtr counters);

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId);

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        EWriteMode writeMode,
        TDuration pbufferReplyTimeout,
        ui64 lsn,
        const NWilson::TTraceId& traceId);

private:
    NActors::TActorSystem* const ActorSystem;
    TVector<std::shared_ptr<TVChunk>> VChunks;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
