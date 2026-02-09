#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>

#include <ydb/core/nbs/cloud/blockstore/libs/service/fast_path_service/storage_transport/storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/request.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// BlocksCount in one vChunk - current limitation
constexpr size_t BlocksCount = 128 * 1024 * 1024 / 4096;

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
{
private:
    struct TBlockMeta {
        TVector<ui64> LsnByPersistentBufferIndex;
        TVector<bool> IsFlushedToDDiskByPersistentBufferIndex;

        explicit TBlockMeta(size_t persistentBufferCount)
            : LsnByPersistentBufferIndex(persistentBufferCount, 0)
            , IsFlushedToDDiskByPersistentBufferIndex(persistentBufferCount, false)
        {}

        void OnWriteCompleted(const TWriteRequestHandler::TPersistentBufferWriteMeta& writeMeta)
        {
            LsnByPersistentBufferIndex[writeMeta.Index] = writeMeta.Lsn;
            IsFlushedToDDiskByPersistentBufferIndex[writeMeta.Index] = false;
        }

        void OnFlushCompleted(size_t persistentBufferIndex, ui64 lsn)
        {
            if (LsnByPersistentBufferIndex[persistentBufferIndex] == lsn) {
                LsnByPersistentBufferIndex[persistentBufferIndex] = 0;
                IsFlushedToDDiskByPersistentBufferIndex[persistentBufferIndex] = true;
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

        TDDiskConnection(const NKikimr::NBsController::TDDiskId& ddiskId,
                         const NKikimr::NDDisk::TQueryCredentials& credentials)
            : DDiskId(ddiskId)
            , Credentials(credentials)
        {}

        [[nodiscard]] NActors::TActorId GetServiceId() const
        {
            return NKikimr::MakeBlobStorageDDiskId(DDiskId.NodeId, DDiskId.PDiskId,
                                          DDiskId.DDiskSlotId);
        }
    };

    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PersistentBufferConnections;

    ui32 BlockSize;
    ui64 BlocksCount; // Currently unused, uses hardcoded BlocksCount

    ui64 TabletId;
    ui32 Generation;
    ui64 StorageRequestId = 0;
    std::unordered_map<ui64, std::shared_ptr<IRequestHandler>> RequestHandlersByStorageRequestId;
    TVector<TBlockMeta> BlocksMeta;
    TQueue<std::shared_ptr<TFlushRequestHandler>> FlushQueue;

    std::unique_ptr<IStorageTransport> StorageTransport;
public:
    TDirectBlockGroup(
        ui64 tabletId,
        ui32 generation,
        TVector<NKikimr::NBsController::TDDiskId> ddisksIds,
        TVector<NKikimr::NBsController::TDDiskId> persistentBufferDDiskIds,
        ui32 blockSize,
        ui64 blocksCount);

    void EstablishConnections();

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request);

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request);

private:
    void HandleConnectResult(
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result);

    void HandleWritePersistentBufferResult(
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult& result);

    void RequestBlockFlush(TWriteRequestHandler& requestHandler);

    void ProcessFlushQueue();

    void HandleFlushPersistentBufferResult(
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvFlushPersistentBufferResult& result);

    void RequestBlockErase(TFlushRequestHandler& requestHandler);

    void HandleErasePersistentBufferResult(
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult& result);

    template <typename TEvent>
    void HandleReadResult(
        ui64 storageRequestId,
        const TEvent& result);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
