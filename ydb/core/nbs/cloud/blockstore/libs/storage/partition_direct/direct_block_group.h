#pragma once

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

struct TRestoreMeta
{
    ui64 BlockIndex = 0;
    ui64 PersistBufferIndex = 0;
    ui64 Lsn = 0;
};

struct TDBGRestoreResponse
{
    NProto::TError Error;
    TVector<TRestoreMeta> Meta;
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

    virtual NThreading::TFuture<TDBGRestoreResponse>
    RestoreFromPersistentBuffers(ui32 vChunkIndex) = 0;
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

    NThreading::TFuture<TDBGRestoreResponse> RestoreFromPersistentBuffers(
        ui32 vChunkIndex) override;

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

        [[nodiscard]] NActors::TActorId GetServiceId() const;
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

    void DoRestore(
        NThreading::TPromise<TDBGRestoreResponse> promise,
        ui32 vChunkIndex);

    NActors::TActorSystem* const ActorSystem = nullptr;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const ui64 TabletId;

    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PBufferConnections;

    std::atomic<ui64> LsnGenerator;   // TODO move to FastPathService

    bool Initialized = false;
    NThreading::TPromise<void> ConnectionEstablishedPromise =
        NThreading::NewPromise();

    std::unique_ptr<NTransport::IStorageTransport> StorageTransport;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
