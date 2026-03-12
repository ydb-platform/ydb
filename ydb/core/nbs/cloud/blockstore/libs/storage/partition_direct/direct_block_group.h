#pragma once

#include "request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Abstract base interface for DirectBlockGroup implementations
class IDirectBlockGroup
{
public:
    virtual ~IDirectBlockGroup() = default;

    virtual NThreading::TFuture<void> EstablishConnections(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex) = 0;

    virtual NThreading::TFuture<TDBGReadBlocksResponse>
    ReadBlocksLocalFromPersistentBuffer(
        ui32 vChunkIndex,
        ui8 persistentBufferIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 lsn) = 0;

    virtual NThreading::TFuture<TDBGReadBlocksResponse>
    ReadBlocksLocalFromDDisk(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksLocal(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) = 0;

    virtual NThreading::TFuture<TDBGSyncBlocksResponse>
    SyncWithPersistentBuffer(
        ui32 vChunkIndex,
        ui8 persistBufferIndex,
        const TVector<TSyncRequest>& syncRequests,
        NWilson::TTraceId traceId) = 0;

    virtual TVector<TRestoreMeta> RestoreFromPersistentBuffers(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex) = 0;
};

using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
    : public IDirectBlockGroup
    , public std::enable_shared_from_this<TDirectBlockGroup>
{
private:
    struct TDDiskConnection
    {
        NKikimr::NBsController::TDDiskId DDiskId;
        NKikimr::NDDisk::TQueryCredentials Credentials;

        TDDiskConnection(
            const NKikimr::NBsController::TDDiskId& ddiskId,
            const NKikimr::NDDisk::TQueryCredentials& credentials)
            : DDiskId(ddiskId)
            , Credentials(credentials)
        {}

        [[nodiscard]] NActors::TActorId GetServiceId() const
        {
            return NKikimr::MakeBlobStorageDDiskId(
                DDiskId.NodeId,
                DDiskId.PDiskId,
                DDiskId.DDiskSlotId);
        }
    };

    NActors::TActorSystem* const ActorSystem = nullptr;
    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PersistentBufferConnections;

    ui64 TabletId;
    ui64 StorageRequestId = 0;
    static constexpr ui32 DDisksNumber = 5;

    bool Initialized = false;

    std::unique_ptr<NTransport::IStorageTransport> StorageTransport;

public:
    TDirectBlockGroup(
        NActors::TActorSystem* actorSystem,
        ui64 tabletId,
        ui32 generation,
        TVector<NKikimr::NBsController::TDDiskId> ddisksIds,
        TVector<NKikimr::NBsController::TDDiskId> persistentBufferDDiskIds);

    ~TDirectBlockGroup() override = default;

    NThreading::TFuture<void> EstablishConnections(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex) override;

    NThreading::TFuture<TDBGReadBlocksResponse>
    ReadBlocksLocalFromPersistentBuffer(
        ui32 vChunkIndex,
        ui8 persistentBufferIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 lsn) override;

    NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksLocalFromDDisk(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksLocal(
        ui32 vChunkIndex,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TDBGSyncBlocksResponse> SyncWithPersistentBuffer(
        ui32 vChunkIndex,
        ui8 persistBufferIndex,
        const TVector<TSyncRequest>& syncRequests,
        NWilson::TTraceId traceId) override;

private:
    void DoEstablishPersistentBufferConnection(
        TExecutorPtr executor,
        size_t i,
        std::shared_ptr<TOverallAckRequestHandler> requestHandler);

    void HandlePersistentBufferConnected(
        size_t index,
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result,
        std::shared_ptr<TOverallAckRequestHandler> requestHandler);

    void DoEstablishDDiskConnection(TExecutorPtr executor, size_t i);

    void HandleDDiskBufferConnected(
        size_t index,
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result);

    void DoWriteBlocksLocal(
        std::shared_ptr<TWriteRequestHandler> requestHandler);

    void HandleSyncWithPersistentBufferResult(
        std::shared_ptr<TSyncAndEraseRequestHandler> requestHandler,
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult&
            result);

    void ErasePersistentBuffer(
        std::shared_ptr<TSyncAndEraseRequestHandler> requestHandler);

    void HandleErasePersistentBufferResult(
        std::shared_ptr<TSyncAndEraseRequestHandler> requestHandler,
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult&
            result);

    void DoReadBlocksLocalFromPersistentBuffer(
        std::shared_ptr<TReadRequestHandler> requestHandler,
        ui8 persistentBufferIndex,
        ui64 lsn);

    void DoReadBlocksLocalFromDDisk(
        std::shared_ptr<TReadRequestHandler> requestHandler);

    template <typename TEvent>
    static void HandleReadResult(
        NActors::TActorSystem* actorSystem,
        std::shared_ptr<TReadRequestHandler> requestHandler,
        ui64 storageRequestId,
        const TEvent& result);

    TVector<TRestoreMeta> RestoreFromPersistentBuffers(
        TExecutorPtr executor,
        NWilson::TTraceId traceId,
        ui32 vChunkIndex) override;

    TVector<TRestoreMeta> DoRestoreFromPersistentBuffers(
        TExecutorPtr executor,
        std::shared_ptr<TOverallAckRequestHandler> requestHandler);

    void HandleListPersistentBufferResultOnRestore(
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& result,
        size_t persistentBufferIndex,
        std::shared_ptr<TOverallAckRequestHandler> requestHandler,
        TVector<TRestoreMeta>* restoreLsnMeta);

    void RestoreFromPersistentBufferFinised(
        NWilson::TTraceId traceId,
        ui32 vChunkIndex);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
