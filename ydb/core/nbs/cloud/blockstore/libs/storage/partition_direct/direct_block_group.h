#pragma once

#include "restore_request.h"

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

struct TDBGReadBlocksResponse
{
    NProto::TError Error;
};

struct TDBGWriteBlocksResponse
{
    NProto::TError Error;
};

struct TDBGWriteBlocksToManyPBuffersResponse
{
    struct TSinglePersistentBufferResult
    {
        ui8 HostIndex = 0;
        NProto::TError Error;
    };

    static TDBGWriteBlocksToManyPBuffersResponse MakeOverallError(
        EWellKnownResultCodes code,
        TString reason);

    TVector<TSinglePersistentBufferResult> Responses;
    NProto::TError OverallError;
};

struct TDBGFlushResponse
{
    TVector<NProto::TError> Errors;
};

struct TDBGEraseResponse
{
    NProto::TError Error;
};

struct TDBGRestoreResponse
{
    struct TRestoreMeta
    {
        ui64 Lsn = 0;
        TBlockRange64 Range;
        ui8 HostIndex = 0;
    };

    NProto::TError Error;
    TVector<TRestoreMeta> Meta;
};

struct TListPBufferMeta
{
    ui32 VChunkIndex = 0;
    ui64 Lsn = 0;
    TBlockRange64 Range;
};

using TListPBufferMetaVector = TVector<TListPBufferMeta>;

struct TListPBufferResponse
{
    NProto::TError Error;
    TListPBufferMetaVector Meta;
};

struct TAggregatedListPBufferResponse
{
    NProto::TError Error;
    TMap<ui8, TListPBufferMetaVector> Meta;
};

struct TDDiskIdLess
{
    using TDDiskId = NKikimrBlobStorage::NDDisk::TDDiskId;
    bool operator()(const TDDiskId& lhs, const TDDiskId& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

// Abstract base interface for DirectBlockGroup implementations
class IDirectBlockGroup
{
public:
    virtual ~IDirectBlockGroup() = default;

    virtual TExecutorPtr GetExecutor() = 0;

    virtual void Schedule(TDuration delay, TCallback callback) = 0;

    virtual std::shared_ptr<NWilson::TSpan> CreateChildSpan(
        const NWilson::TTraceId& traceId,
        TStringBuf name) = 0;

    virtual void EstablishConnections() = 0;

    virtual NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        ui8 hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToDDisk(
        ui32 vChunkIndex,
        ui8 hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>
    WriteBlocksToManyPBuffers(
        ui32 vChunkIndex,
        std::vector<ui8> hostIndexes,
        ui64 lsn,
        TBlockRange64 range,
        TDuration replyTimeout,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    // Batch operation to flush a list of PBuffer entries. It can be executed in
    // two modes - when the source and destination are the same host, and when
    // the source and destination hosts are different. In this case, the
    // operation is performed in pull mode, when the destination host downloads
    // entries from PBuffer and write it to DDisk to self.
    virtual NThreading::TFuture<TDBGFlushResponse> SyncWithPBuffer(
        ui32 vChunkIndex,
        ui8 pbufferHostIndex,   // source host
        ui8 ddiskHostIndex,     // destination host
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) = 0;

    // Batch operation to erase a list of PBuffer entries.
    virtual NThreading::TFuture<TDBGEraseResponse> EraseFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) = 0;

    // Get a list of all entries in PBuffers belonging to a given vChunkIndex.
    virtual NThreading::TFuture<TDBGRestoreResponse> RestoreDBGPBuffers(
        ui32 vChunkIndex) = 0;

    // Query persistent buffer from Node.
    virtual NThreading::TFuture<TListPBufferResponse> ListPBuffers(
        ui8 hostIndex) = 0;
};

using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

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
