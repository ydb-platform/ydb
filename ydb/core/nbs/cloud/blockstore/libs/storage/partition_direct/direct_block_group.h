#pragma once

#include "restore_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/thread_checker.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

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

////////////////////////////////////////////////////////////////////////////////

// Abstract base interface for DirectBlockGroup implementations
class IDirectBlockGroup
{
public:
    virtual ~IDirectBlockGroup() = default;

    virtual TExecutorPtr GetExecutor() = 0;

    virtual ui64 GenerateLsn() = 0;

    virtual NThreading::TFuture<void> EstablishConnections() = 0;

    virtual NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        ui8 hostIndex,
        TBlockRange64 range,
        TGuardedSgList guardedSglist,
        NWilson::TTraceId traceId) = 0;
    virtual NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        TGuardedSgList guardedSglist,
        NWilson::TTraceId traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        TGuardedSgList guardedSglist,
        NWilson::TTraceId traceId) = 0;

    virtual NThreading::TFuture<TDBGFlushResponse> FlushFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        const TVector<TPBufferSegment>& segments,
        NWilson::TTraceId traceId) = 0;

    virtual NThreading::TFuture<TDBGEraseResponse> EraseFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        const TVector<TPBufferSegment>& segments,
        NWilson::TTraceId traceId) = 0;

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
        TExecutorPtr executor,
        ui64 tabletId,
        ui32 generation,
        const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
        const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds);

    ~TDirectBlockGroup() override = default;

    // IDirectBlockGroup implementation

    TExecutorPtr GetExecutor() override;

    ui64 GenerateLsn() override;

    NThreading::TFuture<void> EstablishConnections() override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        ui8 hostIndex,
        TBlockRange64 range,
        TGuardedSgList guardedSglist,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        TGuardedSgList guardedSglist,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        TGuardedSgList guardedSglist,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGFlushResponse> FlushFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        const TVector<TPBufferSegment>& segments,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGEraseResponse> EraseFromPBuffer(
        ui32 vChunkIndex,
        ui8 hostIndex,
        const TVector<TPBufferSegment>& segments,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGRestoreResponse> RestoreDBGPBuffers(
        ui32 vChunkIndex) override;

    NThreading::TFuture<TListPBufferResponse> ListPBuffers(
        ui8 hostIndex) override;

private:
    enum class EConnectionType
    {
        DDisk,
        PBuffer,
    };

    struct TDDiskConnection
    {
        NKikimr::NBsController::TDDiskId DDiskId;
        NKikimr::NDDisk::TQueryCredentials Credentials;
        NThreading::TPromise<NProto::TError> ConnectPromise =
            NThreading::NewPromise<NProto::TError>();
        NThreading::TFuture<NProto::TError> ConnectFuture{
            ConnectPromise.GetFuture()};

        [[nodiscard]] NActors::TActorId GetServiceId() const;
        [[nodiscard]] const NThreading::TFuture<NProto::TError>&
        GetFuture() const;
    };

    void DoEstablishConnections();
    void DoEstablishConnection(
        EConnectionType connectionType,
        size_t index,
        const TDDiskConnection& connection);
    void OnConnectionEstablished(
        EConnectionType connectionType,
        size_t index,
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result);

    void DoListPBuffers();
    void OnPBuffersListed(const TAggregatedListPBufferResponse& response);

    void DoRestore(
        NThreading::TPromise<TDBGRestoreResponse> promise,
        ui32 vChunkIndex);

    NActors::TActorSystem* const ActorSystem = nullptr;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const ui64 TabletId;
    const std::unique_ptr<NTransport::IStorageTransport> StorageTransport;

    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PBufferConnections;

    std::atomic<ui64> LsnGenerator;   // TODO move to FastPathService

    bool Initialized = false;
    NThreading::TPromise<void> ConnectionEstablishedPromise =
        NThreading::NewPromise();

    THashMap<ui32, TDBGRestoreResponse> RestoredPBuffers;
    NThreading::TPromise<void> RestoredPBuffersPromise =
        NThreading::NewPromise();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
