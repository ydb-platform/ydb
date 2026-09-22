#pragma once

#include "direct_block_group.h"
#include "region.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>
<<<<<<< HEAD
=======
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_counters.h>
>>>>>>> 78a20188184 (Field map set optimization (#52565))
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
<<<<<<< HEAD
=======
    const IArenaAllocatorPtr ArenaAllocator;
    // Chaos controllers are indexed by DirectBlockGroup index.
    const TVector<NTransport::IChaosInjectorControlPtr> ChaosInjectorControls;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    const TVector<TRegionPtr> Regions;   // 4 GiB each

    std::atomic<ui64> SequenceGenerator;
    std::atomic<size_t> InflightWriteCount{0};
    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TVolumeCounters Counters;
    TVolumeConfigPtr VolumeConfig;

    TAdaptiveLock DumpLock;
    size_t DumpCount = 0;
    TMap<size_t, TDBGDumpResponse> DebugDumps;

<<<<<<< HEAD
    struct TPBufferCleanupGather
    {
        std::atomic<bool> Active{false};
        TVector<std::optional<ui64>> SafeBarriers;
        std::atomic<size_t> PendingResponses{0};
    };

    TPBufferCleanupGather CleanupGather;
=======
    TAdaptiveLock CopyRangeBucketLock;
    std::optional<TSimpleLeakyBucket> CopyRangeBucket;
>>>>>>> 3b287e3de3d (NBS-7459: own PBuffer cleanup in DirectBlockGroup (#52414))

public:
    TFastPathService(
        NActors::TActorSystem* actorSystem,
        NActors::TActorId partitionActorId,
        ui64 tabletId,
        const TString& diskId,
        ui64 blockCount,
        ui32 blockSize,
        TVector<IDirectBlockGroupPtr> directBlockGroups,
<<<<<<< HEAD
        TVChunkConfigByIndex vChunkConfigs,
=======
        TVector<NTransport::IChaosInjectorControlPtr> chaosInjectorControls,
        const TVChunkConfigs& vChunkConfigs,
        ITouchedProvider* touchedProvider,
        const TDirtyMapStateProtos& dirtyMapStates,
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
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

<<<<<<< HEAD
    void RequestAddHost(size_t directBlockGroupId) override;
=======
    TPersistResultFuture UpdateDirtyMapState(
        ui32 vChunkIndex,
        TDirtyMapStateProto state) override;

    TPersistResultFuture SetVChunkTouched(ui32 vChunkIndex) override;

    void QueryAddHost(
        size_t directBlockGroupId,
        ui32 dbgConnectionsConfigGeneration) override;
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))

    void QueryRemoveHost(
        size_t directBlockGroupId,
        size_t hostIndex,
        ui32 dbgConnectionsConfigGeneration) override;

    ui64 OnWriteStarted() override;

    void OnWriteFinished() override;

    // IDiskStateProvider implementation
    size_t GetInflightWriteCount() const override;

<<<<<<< HEAD
=======
    void StopTablet(const TString& reason) override;

    TDuration TakeVolumeCopyRangeBudget(ui64 byteCount) override;

    void PersistHostHealth(
        size_t directBlockGroupId,
        THostIndex hostIndex,
        EHostHealth oldHealth,
        EHostHealth newHealth) override;

>>>>>>> 8a60344d593 (NBS-7513:  Add Oracle->PartitionDirect PersistHostHealth path (#52610))
    // Read-only info for the monitoring UI.
    [[nodiscard]] TFastPathServiceInfo GetMonInfo() const;

    // Gathers per-DBG monitoring snapshots: one if dbgIndex is set, else all.
    [[nodiscard]] NThreading::TFuture<TVector<TDbgSnapshot>> GatherMonSnapshots(
        std::optional<size_t> dbgIndex) const;

private:
    void ScheduleDirtyMapDebugPrint();
    void QueryDirtyMapDebugDump();
    void OnDebugDump(size_t dbgIndex, TDBGDumpResponse dump);

<<<<<<< HEAD
    void MaybeTriggerPBufferCleanup(ui64 lsn);
    void PBufferCleanup();
    void OnGatherSafeBarrierForErase(
        size_t dbgIndex,
        std::optional<ui64> safeBarrier);
    void FinishPBufferCleanup();
=======
    void ScheduleVChunkCountersUpdate();
    void QueryVChunkStats();
    void OnVChunkStats(const TVChunkStatsGatherResult& result);
>>>>>>> 3b287e3de3d (NBS-7459: own PBuffer cleanup in DirectBlockGroup (#52414))
};

<<<<<<< HEAD
=======
////////////////////////////////////////////////////////////////////////////////

>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
