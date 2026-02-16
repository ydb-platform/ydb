#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/fast_path_service/storage_transport/storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/request.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// Abstract base interface for DirectBlockGroup implementations
class IDirectBlockGroup
{
public:
    virtual ~IDirectBlockGroup() = default;

    virtual void EstablishConnections() = 0;

    virtual NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) = 0;

    virtual NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) = 0;

    virtual void SetWriteBlocksReplyCallback(
        std::function<void(bool)> callback) = 0;
    virtual void SetReadBlocksReplyCallback(
        std::function<void(bool)> callback) = 0;
};

using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

// BlocksCount in one vChunk - current limitation
constexpr size_t BlocksCount = 128 * 1024 * 1024 / 4096;

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
    : public IDirectBlockGroup
    , public std::enable_shared_from_this<TDirectBlockGroup>
{
private:
    struct TBlockMeta
    {
        TVector<ui64> LsnByPersistentBufferIndex;
        TVector<bool> IsFlushedToDDiskByPersistentBufferIndex;

        explicit TBlockMeta(size_t persistentBufferCount)
            : LsnByPersistentBufferIndex(persistentBufferCount, 0)
            , IsFlushedToDDiskByPersistentBufferIndex(
                  persistentBufferCount,
                  false)
        {}

        void OnWriteCompleted(
            const TWriteRequestHandler::TPersistentBufferWriteMeta& writeMeta)
        {
            LsnByPersistentBufferIndex[writeMeta.Index] = writeMeta.Lsn;
            IsFlushedToDDiskByPersistentBufferIndex[writeMeta.Index] = false;
        }

        void OnFlushCompleted(size_t persistentBufferIndex, ui64 lsn)
        {
            if (LsnByPersistentBufferIndex[persistentBufferIndex] == lsn) {
                LsnByPersistentBufferIndex[persistentBufferIndex] = 0;
                IsFlushedToDDiskByPersistentBufferIndex[persistentBufferIndex] =
                    true;
            }
        }

        [[nodiscard]] bool IsWritten() const
        {
            return IsFlushedToDDisk() || LsnByPersistentBufferIndex[0] != 0;
        }

        [[nodiscard]] bool IsFlushedToDDisk() const
        {
            return IsFlushedToDDiskByPersistentBufferIndex[0];
        }
    };

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

    TMutex Lock;

    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PersistentBufferConnections;

    ui64 TabletId;
    ui32 Generation;
    ui32 BlockSize;
    ui64 BlocksCount;   // Currently unused, uses hardcoded BlocksCount
    ui64 StorageRequestId = 0;

    TVector<TBlockMeta> BlocksMeta;
    TQueue<std::shared_ptr<TSyncRequestHandler>> SyncQueue;

    std::function<void(bool)> WriteBlocksReplyCallback;
    std::function<void(bool)> ReadBlocksReplyCallback;

    std::unique_ptr<IStorageTransport> StorageTransport;

public:
    TDirectBlockGroup(
        ui64 tabletId,
        ui32 generation,
        TVector<NKikimr::NBsController::TDDiskId> ddisksIds,
        TVector<NKikimr::NBsController::TDDiskId> persistentBufferDDiskIds,
        ui32 blockSize,
        ui64 blocksCount);

    void SetWriteBlocksReplyCallback(
        std::function<void(bool)> callback) override
    {
        WriteBlocksReplyCallback = std::move(callback);
    }

    void SetReadBlocksReplyCallback(std::function<void(bool)> callback) override
    {
        ReadBlocksReplyCallback = std::move(callback);
    }

    void EstablishConnections() override;

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

private:
    void HandlePersistentBufferConnected(
        size_t index,
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result);
    void HandleDDiskBufferConnected(
        size_t index,
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result);

    void HandleWritePersistentBufferResult(
        std::shared_ptr<TWriteRequestHandler> requestHandler,
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult&
            result);

    void RequestBlockFlush(const TWriteRequestHandler& requestHandler);

    void ProcessSyncQueue();

    void RequestBlockErase(const TSyncRequestHandler& requestHandler);

    void HandleErasePersistentBufferResult(
        std::shared_ptr<TEraseRequestHandler> requestHandler,
        const NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult&
            result);

    template <typename TEvent>
    void HandleReadResult(
        std::shared_ptr<TReadRequestHandler> requestHandler,
        ui64 storageRequestId,
        const TEvent& result);

    void HandleSyncWithPersistentBufferResult(
        std::shared_ptr<TSyncRequestHandler> requestHandler,
        const NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult&
            result);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
