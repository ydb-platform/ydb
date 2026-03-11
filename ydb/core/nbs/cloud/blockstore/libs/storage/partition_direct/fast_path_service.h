#pragma once

#include "region.h"

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/volume_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TFastPathService
    : public IStorage
    , public std::enable_shared_from_this<TFastPathService>
{
private:
    NActors::TActorSystem* const ActorSystem = nullptr;
    const std::shared_ptr<TRegion> Region;

    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TVolumeCounters Counters;

public:
    TFastPathService(
        NActors::TActorSystem* actorSystem,
        ui64 tabletId,
        ui32 generation,
        std::shared_ptr<TRegion> region,
        const NProto::TStorageServiceConfig& storageConfig,
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
