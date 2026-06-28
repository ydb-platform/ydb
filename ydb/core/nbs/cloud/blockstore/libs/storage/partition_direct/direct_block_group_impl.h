#pragma once

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/thread_checker.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ddisk_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
    : public IDirectBlockGroup
    , public IHostStateController
    , public std::enable_shared_from_this<TDirectBlockGroup>
{
public:
    TDirectBlockGroup(
        NActors::TActorSystem* actorSystem,
        TStorageConfigPtr storageConfig,
        TExecutorPtr executor,
        const TString& diskId,
        ui64 tabletId,
        ui32 generation,
        size_t directBlockGroupIndex,
        const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
        const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds,
        std::unique_ptr<NTransport::IStorageTransport> storageTransport);

    ~TDirectBlockGroup() override = default;

    // IDirectBlockGroup implementation

    void Register(TVChunkWeakPtr vChunk) override;

    TExecutorPtr GetExecutor() override;

    IOraclePtr GetOracle() override;

    void Schedule(TDuration delay, TCallback callback) override;

    std::shared_ptr<NWilson::TSpan> CreateChildSpan(
        const NWilson::TTraceId& traceId,
        TStringBuf name) override;

    NThreading::TFuture<void> Run(IPartitionDirectService* service) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToDDisk(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    void WriteBlocksToManyPBuffers(
        ui32 vChunkIndex,
        THostIndex coordinatorHostIndex,
        TVector<THostIndex> hostIndexes,
        ui64 lsn,
        TBlockRange64 range,
        TDuration replyTimeout,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId,
        TWriteBlocksToManyPBuffersCallback callback) override;

    NThreading::TFuture<TDBGFlushResponse> SyncWithPBuffer(
        ui32 vChunkIndex,
        THostIndex pbufferHostIndex,
        THostIndex ddiskHostIndex,
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGEraseResponse> BatchEraseFromPBuffer(
        THostIndex hostIndex,
        const TEraseSegments& segments,
        const NWilson::TTraceId& traceId) override;

    void BarrierEraseFromPBuffer(ui64 lsn) override;

    NThreading::TFuture<std::optional<ui64>>
    GatherSafeBarrierForErase() override;

    NThreading::TFuture<TDBGRestoreResponse> RestoreDBGPBuffers(
        ui32 vChunkIndex) override;

    NThreading::TFuture<TListPBufferResponse> ListPBuffers(
        THostIndex hostIndex) override;

    NThreading::TFuture<TDBGDumpResponse> Dump() override;

    // IHostStateController implementation
    void SetHostState(
        THostIndex hostIndex,
        EHostState oldState,
        EHostState newState) override;
    ui64 GetHostPBufferUsedSize(THostIndex hostIndex) const override;

private:
    using TEvSyncResult = NKikimrBlobStorage::NDDisk::TEvSyncResult;
    using EConnectionType = NTransport::THostConnection::EConnectionType;
    using TDDiskIdToHostIndex =
        TMap<NKikimrBlobStorage::NDDisk::TDDiskId, THostIndex, TDDiskIdLess>;

    // State of a logical session (lock) with a DDisk.
    // Sessions are used only for DDisk connections.
    enum class EDDiskSessionState
    {
        NotLocked,
        Locked,
        Broken,
    };

    struct TDDiskConnection
    {
        using TPromise = NThreading::TPromise<NProto::TError>;
        using TFuture = NThreading::TFuture<NProto::TError>;

        NTransport::THostConnection HostConnection;
        TPromise ConnectPromise = NThreading::NewPromise<NProto::TError>();
        TFuture ConnectFuture{ConnectPromise.GetFuture()};

        EDDiskSessionState SessionState = EDDiskSessionState::NotLocked;

        [[nodiscard]] const TFuture& GetFuture() const;
    };

    void DoEstablishConnections();
    void DoEstablishConnection(
        size_t index,
        const TDDiskConnection& connection);
    void OnConnectionEstablished(
        EConnectionType connectionType,
        size_t index,
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result);

    [[nodiscard]] bool HasPBufferQuorum() const;
    [[nodiscard]] bool HasLockedQuorum() const;

    void DoListPBuffers();
    void OnPBuffersListed(const TAggregatedListPBufferResponse& response);

    void OnWriteBlocksToManyPBuffersResponse(
        const NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffersResult&
            response,
        THostIndex coordinatorHostIndex,
        TWriteBlocksToManyPBuffersCallback callback,
        TDuration executionTime);

    TDBGFlushResponse HandleSyncWithPBufferResponse(
        const TEvSyncResult& response,
        size_t segmentCount);

    void DoBarrierEraseFromPBuffer(
        THostIndex hostIndex,
        ui64 lsn,
        const NWilson::TTraceId& traceId);

    void DoRestore(
        NThreading::TPromise<TDBGRestoreResponse> promise,
        ui32 vChunkIndex);

    // Called right before a request is sent to the given host. Updates the
    // per-host inflight counter for the given operation type.
    void OnRequest(THostIndex hostIndex, EOperation operation);

    void OnResponse(
        THostIndex hostIndex,
        TDuration executionTime,
        EOperation operation,
        const NProto::TError& error);
    void OnMultiFlushResponse(
        THostIndex pbufferHostIndex,
        THostIndex ddiskHostIndex,
        TDuration executionTime,
        const TVector<NProto::TError>& errors);

    void Thinking();
    void ScheduleOracleThinking();

    [[nodiscard]] bool WaitForSessionLock(THostIndex hostIndex);

    TDBGDumpResponse DoDebugPrintDirtyMap();

    NActors::TActorSystem* const ActorSystem = nullptr;
    const TStorageConfigPtr StorageConfig;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const ui64 TabletId;
    const size_t DirectBlockGroupIndex;
    const std::unique_ptr<NTransport::IStorageTransport> StorageTransport;

    TLogTitle LogTitle;
    IPartitionDirectService* Service = nullptr;
    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PBufferConnections;
    TDDiskIdToHostIndex PBufferIdToHostIndex;
    TVector<TVChunkWeakPtr> VChunks;
    TOracle Oracle;

    // One-shot signal of the FIRST time the locked quorum was reached. Used
    // ONLY to gate the synchronous tablet start (wait for readiness before
    // opening the endpoint). It does NOT reflect the current runtime readiness.
    NThreading::TPromise<void> InitialReadyPromise = NThreading::NewPromise();

    THashMap<ui32, TDBGRestoreResponse> RestoredPBuffers;
    NThreading::TPromise<void> RestoredPBuffersPromise =
        NThreading::NewPromise();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
