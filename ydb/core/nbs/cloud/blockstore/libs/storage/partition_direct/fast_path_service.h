#pragma once

#include "region.h"

#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/volume_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;
constexpr size_t BlocksPerRegion = TRegion::RegionSize / BlockSize;

////////////////////////////////////////////////////////////////////////////////

class TFastPathService
    : public IStorage
    , public std::enable_shared_from_this<TFastPathService>
{
private:
    NActors::TActorSystem* const ActorSystem = nullptr;
    const TVector<std::shared_ptr<TRegion>> Regions;   // 4 GiB each

    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TVolumeCounters Counters;

public:
    TFastPathService(
        NActors::TActorSystem* actorSystem,
        ui64 tabletId,
        ui32 generation,
        TVector<std::shared_ptr<TRegion>> regions,
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

    size_t GetRegionIndex(ui64 blockIndex) const;
    size_t GetRegionOffset(ui64 blockIndex) const;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
