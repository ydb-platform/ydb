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

    struct TRequestsStalking
    {
        explicit TRequestsStalking(ui32 responsesExpected)
            : ResponsesExpected(responsesExpected)
        {}

        ui32 ResponsesHandled = 0;
        const ui32 ResponsesExpected;
    };

    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PersistentBufferConnections;

    ui32 BlockSize;
    ui64 BlocksCount; // Currently unused, uses hardcoded BlocksCount

    ui64 TabletId;
    ui32 Generation;
    ui64 StorageRequestId = 0;
    std::unordered_map<ui64, std::shared_ptr<IRequestHandler>> RequestHandlersByStorageRequestId;
    class TDirtyMap;
    std::unique_ptr<TDirtyMap> DirtyMap;
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
    ~TDirectBlockGroup();

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
        const NKikimrBlobStorage::NDDisk::TEvConnectResult& result,
        std::shared_ptr<TRequestsStalking> connectionsStalking);

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

    void RestorePersistentBuffer();
    void HandleListPersistentBufferResultOnRestore(
        ui64 storageRequestId,
        const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& result,
        size_t persistentBufferIndex,
        std::shared_ptr<TRequestsStalking> requestsStalking);
    void RestorePersistentBufferFinised();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
