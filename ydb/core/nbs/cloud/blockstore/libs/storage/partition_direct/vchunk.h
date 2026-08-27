#pragma once

#include "public.h"

#include "ddisk_data_copier.h"
#include "erase_request.h"
#include "flush_request.h"
#include "write_request_bundle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/thread_checker.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_model.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TVChunk
    : public IWriteClient
    , public std::enable_shared_from_this<TVChunk>
{
public:
    TVChunk(
        NActors::TActorSystem* actorSystem,
        ITraceService* traceService,
        IPartitionDirectService* partitionDirectService,
        const TDiskDescription& diskDescription,
        const TVChunkConfig& vChunkConfig,
        const TDirtyMapStateProto& dirtyMapState,
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

    // If the current count of hosts in the config is less than the desired
    // host count, update the config and persist it in the tablet.
    void UpdateHostCount(size_t newHostCount);

    [[nodiscard]] const TVChunkConfig& GetConfig() const;
    [[nodiscard]] TExecutorPtr GetExecutor() const;
    [[nodiscard]] TCountAndSize GetPBuffersUsage(THostIndex hostIndex) const;
    [[nodiscard]] TCountAndSize GetAheadBlocks(THostIndex hostIndex) const;
    [[nodiscard]] TCountAndSize GetBehindBlocks(THostIndex hostIndex) const;

    // This vchunk's contribution to the tablet-wide cleanup watermark: the
    // smallest record id still held in PBuffers, or nullopt when nothing is
    // inflight. Until the dirty map is restored it returns the zero record id
    // (the blocking bound), so the cleanup cannot erase records that are not
    // accounted for yet.
    // Must run on the executor thread.
    [[nodiscard]] std::optional<TPBufferKey> GetSafeBarrierForErase() const;

    [[nodiscard]] TString DebugPrintDirtyMap();

    // Snapshot for the mon page. Must run on the executor thread.
    [[nodiscard]] TVChunkSnapshot BuildMonSnapshot();

    // IWriteClient implementation
    void OnWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        const TWriteRequestResponse& response) override;
    void OnBelatedWriteBlocksResponse(
        std::shared_ptr<TWriteRequestBundle> bundle,
        THostMask completedWrites) override;

private:
    friend struct TBaseFixture;

    using TPrepareConfigFunc = std::function<TVChunkConfig()>;

    struct TPendingVChunkConfig
    {
        TPrepareConfigFunc PrepareConfig;
        TVChunkConfig Config;
        TString Message;
    };

    void UpdateDirtyMap(const TDBGRestoreResponse& response);

    void DoStart();
    void DoStop();
    void OnStopped();

    void DoReadBlocksLocal(
        TTracedPromise<TReadBlocksLocalResponse> promise,
        TBlockRange64 vchunkRange,
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

    void DoPersistDirtyMap();
    void OnDirtyMapPersisted(ui32 stateGeneration);

    void ScheduleCleaningUp();
    void CleaningUp();

    void UpdatePendingCounters();

    // Persists newConfig to the partition's local DB. The in-memory config is
    // unchanged; the new value applies after config persisted.
    void UpdateConfig(TPrepareConfigFunc prepareConfig, TString message);
    void PersistNextPendingConfig();
    void OnConfigPersisted();
    void ApplyConfig(TVChunkConfig newConfig, const TString& message);

    TVChunkConfig PrepareNewConfig(
        THostIndex hostIndex,
        EHostState state) const;

    void OnCopierStopped(
        THostIndex hostIndex,
        TDDiskDataCopier::EResult result);
    void OnCopyComplete(THostIndex hostIndex, TDDiskDataCopier::EResult result);
    void DemoteUnavailableHostsIfNeeded();
    [[nodiscard]] THostMask GetDDisksForDemote() const;

    // Checks DirtyMap's initial readiness and waits it if need.
    void WaitForDirtyMapReady();

    [[nodiscard]] TString PrintHostAndNode(THostIndex host) const;
    [[nodiscard]] TString PrintInflight() const;

    NActors::TActorSystem* const ActorSystem = nullptr;
    ITraceService* const TraceService = nullptr;
    IPartitionDirectService* const PartitionDirectService = nullptr;
    const TDiskDescription DiskDescription;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const IDirectBlockGroupPtr DirectBlockGroup;
    const ui32 BlockSize;
    const ui64 BlocksCount;
    const ui32 SyncRequestsBatchSize;

    TLogTitle LogTitle;
    TVChunkConfig VChunkConfig;
    TList<TPendingVChunkConfig> PendingVChunkConfigs;
    bool DirtyMapStatePersisting = false;
    TBlocksDirtyMapPtr BlocksDirtyMap;
    // One-shot signal of the INITIAL DirtyMap assembly at tablet start.
    NThreading::TPromise<void> DirtyMapReady = NThreading::NewPromise();
    TMap<THostIndex, TDDiskDataCopierPtr> Copiers;

    size_t InflightWritesCount = 0;
    size_t InflightFlushesCount = 0;
    bool CleaningUpScheduled = false;
    bool Stopped = false;

    TVector<IRequestExecutorWeakPtr> Inflight;

    TVChunkCounters Counters;

    NThreading::TPromise<void> StopPromise = NThreading::NewPromise();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
