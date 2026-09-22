#pragma once

#include "public.h"

#include "ddisk_data_copier.h"
#include "erase_request.h"
#include "flush_request.h"
#include "write_request_bundle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/thread_checker.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Owns I/O, synchronization, configuration, and DirtyMap state for one VChunk.
//
// Persistence invariants:
// - At most one config or DirtyMap persist is in flight; queued configs take
//   priority over DirtyMap-only persists.
// - A config is persisted atomically with its predicted DirtyMap state and is
//   applied in memory only after a successful persist. The initial Behind of a
//   newly promoted DDisk is included in that prediction.
// - DirtyMap-only persists leave the config unchanged. Behind changes not
//   covered by a config snapshot advance StateGeneration and are persisted by
//   a later request.
// - PersistedStateGeneration advances and PersistedFreshDDisks changes only
//   after a successful persist.
// - The touched marker is persisted independently.
class TVChunk
    : public IWriteClient
    , public std::enable_shared_from_this<TVChunk>
{
public:
    TVChunk(
        NActors::TActorSystem* actorSystem,
        IPartitionDirectService* partitionDirectService,
        const TVChunkConfig& vChunkConfig,
<<<<<<< HEAD
=======
        bool touched,
        const TDirtyMapStateProto& dirtyMapState,
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
        IDirectBlockGroupPtr directBlockGroup,
        ui32 syncRequestsBatchSize,
        ui64 vChunkSize,
        NMonitoring::TDynamicCounterPtr counters);

    ~TVChunk() override;

    void Start();
    NThreading::TFuture<void> Stop();

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId);

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId);

    void SetHostState(THostIndex hostIndex, EHostState state);

    void OnHostAppended(size_t newHostCount);

    [[nodiscard]] const TVChunkConfig& GetConfig() const;
<<<<<<< HEAD
<<<<<<< HEAD
    [[nodiscard]] ui64 GetPBufferUsedSize(THostIndex hostIndex) const;
=======
=======
    [[nodiscard]] THostMask GetHealthyDDisks() const;
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    [[nodiscard]] TExecutorPtr GetExecutor() const;
    [[nodiscard]] TCountAndSize GetPBuffersUsage(THostIndex hostIndex) const;
    // This vchunk's contribution to the tablet-wide cleanup watermark: the
    // smallest record id still held in PBuffers, or nullopt when nothing is
    // inflight. Until the dirty map is restored it returns the zero record id
    // (the blocking bound), so the cleanup cannot erase records that are not
    // accounted for yet.
    // Must run on the executor thread.
    [[nodiscard]] std::optional<TPBufferKey> GetSafeBarrierForErase() const;

>>>>>>> 78a20188184 (Field map set optimization (#52565))
    [[nodiscard]] TString DebugPrintDirtyMap();
    [[nodiscard]] TDirtyMapStats GetDirtyMapStats() const;
    [[nodiscard]] TDirtyMapHostStats GetDirtyMapHostStats(
        THostIndex hostIndex) const;

    // This vchunk's contribution to the tablet-wide cleanup watermark: the
    // smallest lsn still held in PBuffers, or nullopt when nothing is inflight.
    // Until the dirty map is restored it returns 0 (the blocking bound), so
    // the cleanup cannot erase records that are not accounted for yet.
    // Must run on the executor thread.
    [[nodiscard]] std::optional<ui64> GetSafeBarrierForErase() const;

    // IWriteClient implementation
    void OnWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        const TWriteRequestResponse& response) override;
    void OnBelatedWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        THostMask completedWrites) override;

<<<<<<< HEAD
=======
    // IRangeSyncClient implementation
    [[nodiscard]] std::optional<TBlockRange16> GetFreshRange(
        THostIndex host) const override;
    [[nodiscard]] TReadHint MakeReadHint(TBlockRange16 range) override;
    [[nodiscard]] TRangeLock MakeDDiskRangeLock(
        TBlockRange16 range,
        THostMask mask) override;
    TSyncHint BeginRangeSync(THostIndex host, TBlockRange16 range) override;
    void EndRangeSync(ui64 syncId, bool success) override;
    void OnCopyProgress(ui64 totalBytes) override;

>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
private:
    friend struct TBaseFixture;

    enum class ETouchedState
    {
        // No writes to the VChunk's DDisks have been observed.
        NotTouched,
        // A write was observed and the touched marker is being persisted.
        Persisting,
        // The touched marker was successfully persisted.
        Persisted
    };

    using TPrepareConfigFunc = std::function<TVChunkConfig()>;
    using TApplyPersistedConfigFunc = std::function<void()>;

    struct TPendingVChunkConfig
    {
        TPrepareConfigFunc PrepareConfig;
<<<<<<< HEAD
        TApplyPersistedConfigFunc ApplyPersisted;

        TVChunkConfig Config;
=======
        TString Message;
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    };

    void UpdateDirtyMap(const TDBGRestoreResponse& response);

    void DoStart();
    void DoStop();

    void DoReadBlocksLocal(
        TTracedPromise<TReadBlocksLocalResponse> promise,
        TBlockRange16 vchunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        std::shared_ptr<NWilson::TSpan> span);
    void OnReadBlocksResponse(const IReadRequestExecutor::TResponse& response);

    void DoWriteBlocksLocal(std::shared_ptr<TWriteRequestBundle> bundle);
    void DoFlush(bool force);
    void OnFlushResponse(const TFlushRequestExecutor::TResponse& response);

    void DoErase(bool force, TBlocksDirtyMap::EEraseType eraseType);
    void OnEraseResponse(const TEraseRequestExecutor::TResponse& response);
    void OnEraseBelatedResponse(
        const TEraseRequestExecutor::TResponse& response);

<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
    void StartPersist();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    void DoPersistDirtyMap();
    void OnDirtyMapPersisted(ui32 stateGeneration, THostMask freshDDisks);

    // VDisk touch state.
    void Touch();
    [[nodiscard]] bool IsTouched() const;
    void DoPersistTouched();
    void OnTouchedPersisted();

>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
    void ScheduleCleaningUp();
    void CleaningUp();

    void UpdatePendingCounters();

    // Persists newConfig to the partition's local DB. The in-memory config is
    // unchanged; the new value applies after config persisted.
    void UpdateConfig(
        TPrepareConfigFunc prepareConfig,
        TApplyPersistedConfigFunc applyPersisted);
    void PersistNextPendingConfig();
<<<<<<< HEAD
    void OnConfigPersisted();
=======
    void OnConfigPersisted(
        const TVChunkConfig& config,
        const TString& message,
        ui32 stateGeneration,
        THostMask freshDDisks);
    void ApplyConfig(const TVChunkConfig& newConfig, const TString& message);
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))

    TVChunkConfig PrepareNewConfig(
        THostIndex hostIndex,
        EHostState state) const;
    void ApplyConfig();

    void OnCopierStopped(
        THostIndex hostIndex,
        TDDiskDataCopier::EResult result);
    void OnCopyComplete(THostIndex hostIndex, TDDiskDataCopier::EResult result);

    // Checks DirtyMap's initial readiness and waits it if need.
    void WaitForDirtyMapReady();

    [[nodiscard]] TString PrintInflight() const;

    NActors::TActorSystem* const ActorSystem = nullptr;
    IPartitionDirectService* const PartitionDirectService = nullptr;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const IDirectBlockGroupPtr DirectBlockGroup;
    const ui32 BlockSize;
    const ui16 BlocksCount;
    const ui32 SyncRequestsBatchSize;

    TLogTitle LogTitle;
    TVChunkConfig VChunkConfig;
    TList<TPendingVChunkConfig> PendingVChunkConfigs;
<<<<<<< HEAD
<<<<<<< HEAD
    TBlocksDirtyMap BlocksDirtyMap;
=======
    bool DirtyMapStatePersisting = false;
    ETouchedState TouchedState = ETouchedState::NotTouched;
    TBlocksDirtyMapPtr BlocksDirtyMap;
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
=======
    ETouchedState TouchedState = ETouchedState::NotTouched;
    TBlocksDirtyMapPtr BlocksDirtyMap;
    THostMask PersistedFreshDDisks;
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    // One-shot signal of the INITIAL DirtyMap assembly at tablet start.
    NThreading::TPromise<void> DirtyMapReady = NThreading::NewPromise();
    TMap<THostIndex, TDDiskDataCopierPtr> Copiers;

    size_t InflightWritesCount = 0;
    size_t InflightFlushesCount = 0;
    bool CleaningUpScheduled = false;
    bool Stopped = false;
    bool Persisting = false;

    TVector<IRequestExecutorWeakPtr> Inflight;

    TVChunkCounters Counters;

    NThreading::TPromise<void> StopPromise = NThreading::NewPromise();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
