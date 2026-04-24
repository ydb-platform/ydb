#pragma once

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/thread_checker.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
    : public IDirectBlockGroup
    , public std::enable_shared_from_this<TDirectBlockGroup>
{
public:
    TDirectBlockGroup(
        NActors::TActorSystem* actorSystem,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        TExecutorPtr executor,
        ui64 tabletId,
        ui32 generation,
        const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
        const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds);

    ~TDirectBlockGroup() override = default;

    // IDirectBlockGroup implementation

    TExecutorPtr GetExecutor() override;

    void Schedule(TDuration delay, TCallback callback) override;

    std::shared_ptr<NWilson::TSpan> CreateChildSpan(
        const NWilson::TTraceId& traceId,
        TStringBuf name) override;

    void EstablishConnections() override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        ui8 hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToDDisk(
        ui32 vChunkIndex,
        ui8 hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>
    WriteBlocksToManyPBuffers(
        ui32 vChunkIndex,
        std::vector<ui8> hostIndexes,
        ui64 lsn,
        TBlockRange64 range,
        TDuration replyTimeout,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGFlushResponse> SyncWithPBuffer(
        ui32 vChunkIndex,
        ui8 pbufferHostIndex,
        ui8 ddiskHostIndex,
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGEraseResponse> EraseFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) override;

    NThreading::TFuture<TDBGRestoreResponse> RestoreDBGPBuffers(
        ui32 vChunkIndex) override;

    NThreading::TFuture<TListPBufferResponse> ListPBuffers(
        ui8 hostIndex) override;

private:
    using EConnectionType = NTransport::THostConnection::EConnectionType;
    using TDDiskIdToHostIndex =
        TMap<NKikimrBlobStorage::NDDisk::TDDiskId, ui8, TDDiskIdLess>;

    struct TDDiskConnection
    {
        using TPromise = NThreading::TPromise<NProto::TError>;
        using TFuture = NThreading::TFuture<NProto::TError>;

        NTransport::THostConnection HostConnection;
        TPromise ConnectPromise = NThreading::NewPromise<NProto::TError>();
        TFuture ConnectFuture{ConnectPromise.GetFuture()};

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

    void DoListPBuffers();
    void OnPBuffersListed(const TAggregatedListPBufferResponse& response);

    void OnWriteBlocksToManyPBuffersResponse(
        const NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffersResult&
            response,
        NThreading::TPromise<TDBGWriteBlocksToManyPBuffersResponse> promise,
        TDuration executionTime);

    void DoRestore(
        NThreading::TPromise<TDBGRestoreResponse> promise,
        ui32 vChunkIndex);

    void OnResponse(
        ui8 hostIndex,
        TDuration executionTime,
        EOperation operation,
        const NProto::TError& error);
    void OnMultiFlushResponse(
        ui8 pbufferHostIndex,
        ui8 ddiskHostIndex,
        TDuration executionTime,
        const TVector<NProto::TError>& errors);

    NActors::TActorSystem* const ActorSystem = nullptr;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const ui64 TabletId;
    const std::unique_ptr<NTransport::IStorageTransport> StorageTransport;

    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PBufferConnections;
    TVector<THostStat> HostStatistics;
    TDDiskIdToHostIndex PBufferIdToHostIndex;

    bool Initialized = false;
    NThreading::TPromise<void> ConnectionEstablishedPromise =
        NThreading::NewPromise();

    THashMap<ui32, TDBGRestoreResponse> RestoredPBuffers;
    NThreading::TPromise<void> RestoredPBuffersPromise =
        NThreading::NewPromise();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
