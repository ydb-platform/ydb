#pragma once

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/volume_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/region.h>

#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/mon/mon.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TFastPathService
    : public IStorage
    , public std::enable_shared_from_this<TFastPathService>
{
private:
    TMutex Lock;
    NActors::TActorSystem* const ActorSystem = nullptr;
    std::shared_ptr<NStorage::NPartitionDirect::TRegion> Region;

    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TVolumeCounters Counters;

public:
    TFastPathService(
        NActors::TActorSystem* actorSystem,
        ui64 tabletId,
        ui32 generation,
        std::shared_ptr<NStorage::NPartitionDirect::TRegion> region,
        const NYdb::NBS::NProto::TStorageServiceConfig& storageConfig,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters = nullptr);

    ~TFastPathService() override = default;

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) override;

    NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) override;

    void ReportIOError() override;

private:
    NWilson::TTraceId SpanTrace();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
