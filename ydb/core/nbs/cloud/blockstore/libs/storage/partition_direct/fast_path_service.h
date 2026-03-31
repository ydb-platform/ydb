#pragma once

#include "region.h"

#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/volume_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TFastPathService
    : public IStorage
    , public IPartitionDirectService
    , public std::enable_shared_from_this<TFastPathService>
{
private:
    NActors::TActorSystem* const ActorSystem = nullptr;
    const TString DiskId;
    const TVector<std::shared_ptr<TRegion>> Regions;   // 4 GiB each

    std::atomic<ui64> SequenceGenerator;
    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TVolumeCounters Counters;
    TVolumeConfigPtr VolumeConfig;

public:
    TFastPathService(
        NActors::TActorSystem* actorSystem,
        ui64 tabletId,
        const TString& diskId,
        ui64 blockCount,
        ui32 blockSize,
        TVector<IDirectBlockGroupPtr> directBlockGroups,
        const NProto::TStorageServiceConfig& storageConfig,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters = nullptr);

    ~TFastPathService() override = default;

    // IStorage implementation
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

    // IPartitionDirectService implementation
    TVolumeConfigPtr GetVolumeConfig() const override;
    NWilson::TSpan CreteRootSpan(TStringBuf name) override;

private:
    ui64 GenerateSequenceNumber();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
