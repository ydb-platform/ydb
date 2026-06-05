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
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

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
        IPartitionDirectService* partitionDirectService,
        const TVChunkConfig& vChunkConfig,
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
        ui64 lsn,
        const NWilson::TTraceId& traceId);

    void SetHostState(THostIndex hostIndex, EHostState state);

    [[nodiscard]] const TVChunkConfig& GetConfig() const;
    [[nodiscard]] ui64 GetPBufferUsedSize(THostIndex hostIndex) const;
    [[nodiscard]] TString DebugPrintDirtyMap();

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
    using TApplyPersistedConfigFunc = std::function<void()>;

    struct TPendingVChunkConfig
    {
        TPrepareConfigFunc PrepareConfig;
        TApplyPersistedConfigFunc ApplyPersisted;

        TVChunkConfig Config;
    };

    void UpdateDirtyMap(const TDBGRestoreResponse& response);

    void DoStart();
    void DoStop();

    void DoReadBlocksLocal(
        TTracedPromise<TReadBlocksLocalResponse> promise,
        TBlockRange64 vchunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        std::shared_ptr<NWilson::TSpan> span);

    void DoWriteBlocksLocal(std::shared_ptr<TWriteRequestBundle> bundle);
    void DoFlush(bool force);
    void OnFlushResponse(const TFlushRequestExecutor::TResponse& response);

    void DoErase(bool force, TBlocksDirtyMap::EEraseType eraseType);
    void OnEraseResponse(const TEraseRequestExecutor::TResponse& response);
    void OnEraseBelatedResponse(
        const TEraseRequestExecutor::TResponse& response);

    void ScheduleCleaningUp();
    void CleaningUp();

    void UpdatePendingCounters();

    // Persists newConfig to the partition's local DB. The in-memory config is
    // unchanged; the new value applies after config persisted.
    void UpdateConfig(
        TPrepareConfigFunc prepareConfig,
        TApplyPersistedConfigFunc applyPersisted);
    void PersistNextPendingConfig();
    void OnConfigPersisted();

    TVChunkConfig PrepareNewConfig(
        THostIndex hostIndex,
        EHostState state) const;
    void ApplyConfig();

    void OnCopyComplete(THostIndex hostIndex, TDDiskDataCopier::EResult result);

    NActors::TActorSystem* const ActorSystem = nullptr;
    IPartitionDirectService* const PartitionDirectService = nullptr;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const IDirectBlockGroupPtr DirectBlockGroup;
    const ui32 BlockSize;
    const ui64 BlocksCount;
    const ui32 SyncRequestsBatchSize;

    TLogTitle LogTitle;
    TVChunkConfig VChunkConfig;
    TList<TPendingVChunkConfig> PendingVChunkConfigs;
    TBlocksDirtyMap BlocksDirtyMap;
    bool DirtyMapRestored = false;
    TMap<THostIndex, TDDiskDataCopierPtr> Copiers;

    size_t InflightWritesCount = 0;
    size_t InflightFlushesCount = 0;
    bool CleaningUpScheduled = false;

    TVChunkCounters Counters;

    NThreading::TPromise<void> StopPromise = NThreading::NewPromise();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
