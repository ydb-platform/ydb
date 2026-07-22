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

    // Result of the last finished cleanup round: the minimum safe barrier
    // across all DBGs. 0 until the first round finishes.
    std::atomic<ui64> LastSafeBarrier{0};

    // Last persistent-buffer barrier lsn sent to each distinct pbuffer endpoint
    // (keyed by its service actor id). The tablet-wide barrier reaches an
    // endpoint through every DBG that shares it, so this deduplicates the
    // sends: each endpoint receives the barrier once per advance. Guarded
    // because DBGs consult it from their own executor threads.
    TAdaptiveLock PBufferBarrierLock;
    TMap<NActors::TActorId, ui64> LastSentBarrierByPBuffer;

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

    void QueryAddHost(size_t directBlockGroupId, size_t newHostIndex) override;

    ui64 GenerateLsn() override;

    void StopTablet(const TString& reason) override;

    bool TryAdvancePBufferBarrier(
        const NActors::TActorId& pbufferServiceId,
        ui64 lsn) override;

    // Read-only info for the monitoring UI.
    [[nodiscard]] TFastPathServiceInfo GetMonInfo() const;

    // Gathers per-DBG monitoring snapshots: one if dbgIndex is set, else all.
    [[nodiscard]] NThreading::TFuture<TVector<TDbgSnapshot>> GatherMonSnapshots(
        std::optional<size_t> dbgIndex) const;

    // Snapshot of one vchunk by its global index, built on the owning DBG's
    // executor. Resolves to nullopt when there is no such vchunk.
    [[nodiscard]] NThreading::TFuture<std::optional<TVChunkSnapshot>>
    GatherVChunkMonSnapshot(ui32 vchunkIndex) const;

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

////////////////////////////////////////////////////////////////////////////////

size_t CalcRegionCount(ui64 blockCount, ui32 blockSize);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
