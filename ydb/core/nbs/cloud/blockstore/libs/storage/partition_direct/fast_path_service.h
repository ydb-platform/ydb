#pragma once

#include "direct_block_group.h"
#include "region.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/volume_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TFastPathService
    : public IStorage
    , public IPartitionDirectService
    , public std::enable_shared_from_this<TFastPathService>
{
private:
    NActors::TActorSystem* const ActorSystem = nullptr;
    const TStorageConfigPtr StorageConfig;
    const TString DiskId;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TVector<IDirectBlockGroupPtr> DirectBlockGroups;
    const TVector<std::shared_ptr<TRegion>> Regions;   // 4 GiB each
    const EWriteMode WriteMode;
    const TDuration PBufferReplyTimeout;

    std::atomic<ui64> SequenceGenerator;
    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TVolumeCounters Counters;
    TVolumeConfigPtr VolumeConfig;

    TAdaptiveLock DumpLock;
    size_t DumpCount = 0;
    TMap<size_t, TDBGDumpResponse> DebugDumps;

public:
    TFastPathService(
        NActors::TActorSystem* actorSystem,
        ui64 tabletId,
        const TString& diskId,
        ui64 blockCount,
        ui32 blockSize,
        TVector<IDirectBlockGroupPtr> directBlockGroups,
        TStorageConfigPtr storageConfig,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

    ~TFastPathService() override;

    void Run();

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

    void ScheduleAfterDelay(
        NYdb::NBS::TExecutorPtr executor,
        TDuration delay,
        NYdb::NBS::TCallback callback) override;

private:
    ui64 GenerateSequenceNumber();
    void ScheduleDirtyMapDebugPrint();
    void QueryDirtyMapDebugDump();
    void OnDebugDump(size_t dbgIndex, TDBGDumpResponse dump);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
