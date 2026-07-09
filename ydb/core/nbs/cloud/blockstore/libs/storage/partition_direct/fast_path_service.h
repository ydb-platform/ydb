#pragma once

#include "direct_block_group.h"
#include "region.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/volume_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_model.h>

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
    const NActors::TActorId PartitionActorId;
    const TStorageConfigPtr StorageConfig;
    const TString DiskId;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TVector<IDirectBlockGroupPtr> DirectBlockGroups;
    const TVector<TRegionPtr> Regions;   // 4 GiB each

    std::atomic<ui64> SequenceGenerator;
    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TVolumeCounters Counters;
    TVolumeConfigPtr VolumeConfig;

    TAdaptiveLock DumpLock;
    size_t DumpCount = 0;
    TMap<size_t, TDBGDumpResponse> DebugDumps;

    struct TPBufferCleanupGather
    {
        std::atomic<bool> Active{false};
        TVector<std::optional<ui64>> SafeBarriers;
        std::atomic<size_t> PendingResponses{0};
    };

    TPBufferCleanupGather CleanupGather;

public:
    TFastPathService(
        NActors::TActorSystem* actorSystem,
        NActors::TActorId partitionActorId,
        ui64 tabletId,
        const TString& diskId,
        ui64 blockCount,
        ui32 blockSize,
        TVector<IDirectBlockGroupPtr> directBlockGroups,
        TVChunkConfigByIndex vChunkConfigs,
        TStorageConfigPtr storageConfig,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

    ~TFastPathService() override;

    // Starts all DBGs and regions; returns a future that becomes ready the
    // first time the Locked-session quorum is reached in every DBG.
    NThreading::TFuture<void> Run();
    NThreading::TFuture<void> Stop();

    [[nodiscard]] const TVector<IDirectBlockGroupPtr>&
    GetDirectBlockGroups() const
    {
        return DirectBlockGroups;
    }

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

    void UpdateVChunkConfig(const TVChunkConfig& cfg) override;

    void RequestAddHost(size_t directBlockGroupId) override;

    ui64 GenerateLsn() override;

    // Read-only info for the monitoring UI.
    [[nodiscard]] TFastPathServiceInfo GetMonInfo() const;

private:
    void ScheduleDirtyMapDebugPrint();
    void QueryDirtyMapDebugDump();
    void OnDebugDump(size_t dbgIndex, TDBGDumpResponse dump);

    void MaybeTriggerPBufferCleanup(ui64 lsn);
    void PBufferCleanup();
    void OnGatherSafeBarrierForErase(
        size_t dbgIndex,
        std::optional<ui64> safeBarrier);
    void FinishPBufferCleanup();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
