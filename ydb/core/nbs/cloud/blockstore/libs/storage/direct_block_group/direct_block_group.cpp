#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/fast_path_service/storage_transport/ic_storage_transport.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddisksIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds,
    ui32 blockSize,
    ui64 blocksCount)
    : BlockSize(blockSize)
    , BlocksCount(blocksCount)
    , TabletId(tabletId)
    , Generation(generation)
    , BlocksMeta(BlocksCount, TBlockMeta(persistentBufferDDiskIds.size()))
    , StorageTransport(std::make_unique<TICStorageTransport>())
{
    Y_UNUSED(TabletId);
    Y_UNUSED(Generation);
    Y_UNUSED(BlockSize);
    Y_UNUSED(BlocksCount);
    Y_UNUSED(StorageRequestId);

    auto addDDiskConnections = [&](TVector<NBsController::TDDiskId> ddisksIds,
                                   TVector<TDDiskConnection>& ddiskConnections,
                                   bool fromPersistentBuffer)
    {
        for (const auto& ddiskId: ddisksIds) {
            ddiskConnections.emplace_back(
                ddiskId,
                NDDisk::TQueryCredentials(
                    tabletId,
                    generation,
                    std::nullopt,
                    fromPersistentBuffer));
        }
    };

    // Now we assume that ddisksIds and persistentBufferDDiskIds have the same size
    // since we flush each persistent buffer to ddisk with the same index
    Y_ABORT_UNLESS(ddisksIds.size() == persistentBufferDDiskIds.size());

    addDDiskConnections(std::move(ddisksIds), DDiskConnections, false);
    addDDiskConnections(std::move(persistentBufferDDiskIds), PersistentBufferConnections, true);
}

void TDirectBlockGroup::EstablishConnections()
{
    auto numberConnectionsEstablised = std::make_shared<ui32>(0);
    auto sendConnectRequests =
        [&, numberConnectionsEstablised = numberConnectionsEstablised](
            const TVector<TDDiskConnection>& connections,
            ui64 startRequestId = 0)
    {
        for (size_t i = 0; i < connections.size(); i++) {
            auto future = StorageTransport->Connect(
                connections[i].GetServiceId(),
                connections[i].Credentials,
                startRequestId + i);

            future.Subscribe(
                [this, requestId = startRequestId + i,
                 numberConnectionsEstablised = numberConnectionsEstablised](
                    const auto& f)
                {
                    HandleConnectResult(requestId, f.GetValue(),
                                        numberConnectionsEstablised);
                });
        }
    };

    sendConnectRequests(PersistentBufferConnections);
    // Send connect requests to ddisks with offset by persistent buffer connections count
    sendConnectRequests(DDiskConnections, PersistentBufferConnections.size());
}

void TDirectBlockGroup::HandleConnectResult(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result,
    std::shared_ptr<ui32> numberConnectionsEstablised)
{
    TDDiskConnection* ddiskConnection = nullptr;
    if (storageRequestId < PersistentBufferConnections.size()) {
        ddiskConnection = &PersistentBufferConnections[storageRequestId];
    } else {
        ddiskConnection = &DDiskConnections[storageRequestId - PersistentBufferConnections.size()];
    }

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        ddiskConnection->Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
        *numberConnectionsEstablised += 1;
    } else {
        Y_ABORT("TDirectBlockGroup::HandleConnectResult: connection failed - unhandled error");
    }

    // TODO сделать более красивую проверку
    bool allConnectionsEstablised = *numberConnectionsEstablised == DDiskConnections.size() + PersistentBufferConnections.size();
    if (allConnectionsEstablised) {
        RestorePersistentBuffer();
    }
}

// TODO заблокировать IO до полного восстановления (где-то раньше)
void TDirectBlockGroup::RestorePersistentBuffer()
{
    // TODO пройти по всем PB

    size_t numberOfRequests = 3;
    auto counters = std::make_shared<std::pair<size_t, size_t>>(0, numberOfRequests);
    for (size_t i = 0; i < numberOfRequests; i++) {
        const auto& ddiskConnection = PersistentBufferConnections[i];
        ++StorageRequestId;

        auto future = StorageTransport->ListPersistentBuffer(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            StorageRequestId);

        future.Subscribe(
            [this, requestId = StorageRequestId,
             counters = counters](const auto& f)
            {
                const auto& result = f.GetValue();
                HandleListPersistentBufferResultOnRestore(requestId, result,
                                                          counters);
            });
    }
}

void TDirectBlockGroup::HandleListPersistentBufferResultOnRestore(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& result,
    std::shared_ptr<std::pair<size_t, size_t>> requestsCounters)
{
    Y_UNUSED(storageRequestId);
    ++requestsCounters->first;

    Y_ABORT_UNLESS(result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

    // TODO Handle this response with mutex. There is multithreading access from future's threads

    if (requestsCounters->first == requestsCounters->second) {
        RestorePersistentBufferFinised(); // finish actor bootstrap
    }
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TWriteBlocksLocalResponse> TDirectBlockGroup::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TWriteRequestHandler>(std::move(request));

    for (size_t i = 0; i < 3; i++) {
        const auto& ddiskConnection = PersistentBufferConnections[i];
        ++StorageRequestId;

        auto future = StorageTransport->WritePersistentBuffer(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                requestHandler->GetStartOffset(),
                requestHandler->GetSize()),
            StorageRequestId,   // lsn
            NKikimr::NDDisk::TWriteInstruction(0),
            requestHandler->GetData(),
            StorageRequestId);

        future.Subscribe([this, requestId = StorageRequestId](const auto& f) {
            const auto& result = f.GetValue();
            HandleWritePersistentBufferResult(requestId, result);
        });

        RequestHandlersByStorageRequestId[StorageRequestId] = requestHandler;
        requestHandler->OnWriteRequested(
            StorageRequestId,
            i, // persistentBufferIndex
            StorageRequestId
        );
    }

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::HandleWritePersistentBufferResult(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult& result)
{
    // That means that request is already completed
    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TWriteRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        if (requestHandler.IsCompleted(storageRequestId)) {
            auto& blockMeta = BlocksMeta[requestHandler.GetStartIndex()];
            const auto& writesMeta = requestHandler.GetWritesMeta();
            for (const auto& meta : writesMeta) {
                blockMeta.OnWriteCompleted(meta);
            }

            RequestBlockFlush(requestHandler);

            RequestHandlersByStorageRequestId.erase(storageRequestId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDirectBlockGroup::RequestBlockFlush(
    TWriteRequestHandler& requestHandler)
{
    const auto& blockMeta = BlocksMeta[requestHandler.GetStartIndex()];
    for (size_t i = 0; i < 3; i++) {
        auto flushRequestHandler = std::make_shared<TFlushRequestHandler>(
            requestHandler.GetStartIndex(),
            i, // persistentBufferIndex
            blockMeta.LsnByPersistentBufferIndex[i]
        );

        FlushQueue.push(flushRequestHandler);
    }

    ProcessFlushQueue();
}

void TDirectBlockGroup::ProcessFlushQueue()
{
    if (!FlushQueue.empty()) {
        const auto& flushRequestHandler = FlushQueue.front();
        auto persistentBufferIndex = flushRequestHandler->GetPersistentBufferIndex();
        const auto& ddiskConnection = DDiskConnections[persistentBufferIndex];

        ++StorageRequestId;

        auto future = StorageTransport->FlushPersistentBuffer(
            PersistentBufferConnections[persistentBufferIndex].GetServiceId(),
            PersistentBufferConnections[persistentBufferIndex].Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                flushRequestHandler->GetStartOffset(),
                flushRequestHandler->GetSize()),
            flushRequestHandler->GetLsn(),
            std::make_tuple(
                ddiskConnection.DDiskId.NodeId,
                ddiskConnection.DDiskId.PDiskId,
                ddiskConnection.DDiskId.DDiskSlotId),
            ddiskConnection.Credentials.DDiskInstanceGuid.value(),
            StorageRequestId);

        future.Subscribe([this, requestId = StorageRequestId](const auto& f) {
            const auto& result = f.GetValue();
            HandleFlushPersistentBufferResult(requestId, result);
        });

        RequestHandlersByStorageRequestId[StorageRequestId] = flushRequestHandler;
    }
}

void TDirectBlockGroup::HandleFlushPersistentBufferResult(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvFlushPersistentBufferResult& result)
{
    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TFlushRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        BlocksMeta[requestHandler.GetStartIndex()].OnFlushCompleted(
            requestHandler.GetPersistentBufferIndex(),
            requestHandler.GetLsn());

        FlushQueue.pop();

        ProcessFlushQueue();

        RequestBlockErase(requestHandler);

        RequestHandlersByStorageRequestId.erase(storageRequestId);
    }
}

void TDirectBlockGroup::RequestBlockErase(
    TFlushRequestHandler& requestHandler)
{
    auto eraseRequestHandler = std::make_shared<TEraseRequestHandler>(
        requestHandler.GetStartIndex(),
        requestHandler.GetPersistentBufferIndex(),
        requestHandler.GetLsn()
    );

    ++StorageRequestId;

    auto future = StorageTransport->ErasePersistentBuffer(
        PersistentBufferConnections[requestHandler.GetPersistentBufferIndex()]
            .GetServiceId(),
        PersistentBufferConnections[requestHandler.GetPersistentBufferIndex()]
            .Credentials,
        NKikimr::NDDisk::TBlockSelector(
            0,   // vChunkIndex
            eraseRequestHandler->GetStartOffset(),
            eraseRequestHandler->GetSize()),
        requestHandler.GetLsn(),
        StorageRequestId);

    future.Subscribe([this, requestId = StorageRequestId](const auto& f) {
        const auto& result = f.GetValue();
        HandleErasePersistentBufferResult(requestId, result);
    });

    RequestHandlersByStorageRequestId[StorageRequestId] = eraseRequestHandler;
}

void TDirectBlockGroup::HandleErasePersistentBufferResult(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult& result)
{
    // That means that request is already completed
    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        RequestHandlersByStorageRequestId.erase(storageRequestId);
    }
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse> TDirectBlockGroup::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TReadRequestHandler>(std::move(request));

    auto startIndex = requestHandler->GetStartIndex();

    // Block is not writed
    if (!BlocksMeta[startIndex].IsWritten())
    {
        auto promise = NThreading::NewPromise<TReadBlocksLocalResponse>();

        promise.SetValue(TReadBlocksLocalResponse());

        return promise.GetFuture();
    }

    ++StorageRequestId;

    if (!BlocksMeta[startIndex].IsFlushedToDDisk()) {
        const auto& ddiskConnection = PersistentBufferConnections[0];

        auto future = StorageTransport->ReadPersistentBuffer(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                requestHandler->GetStartOffset(),
                requestHandler->GetSize()),
            BlocksMeta[startIndex].LsnByPersistentBufferIndex[0],
            NKikimr::NDDisk::TReadInstruction(true),
            requestHandler->GetData(),
            StorageRequestId);

        future.Subscribe([this, requestId = StorageRequestId](const auto& f) {
            const auto& result = f.GetValue();
            HandleReadResult(requestId, result);
        });
    } else {
        const auto& ddiskConnection = DDiskConnections[0];

        auto future = StorageTransport->Read(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                requestHandler->GetStartOffset(),
                requestHandler->GetSize()),
            NKikimr::NDDisk::TReadInstruction(true),
            requestHandler->GetData(),
            StorageRequestId);   // requestId

        future.Subscribe([this, requestId = StorageRequestId](const auto& f) {
            const auto& result = f.GetValue();
            HandleReadResult(requestId, result);
        });
    }

    RequestHandlersByStorageRequestId[StorageRequestId] = requestHandler;
    return requestHandler->GetFuture();
}

template <typename TEvent>
void TDirectBlockGroup::HandleReadResult(
    ui64 storageRequestId,
    const TEvent& result)
{
    // That means that request is already completed
    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TReadRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        if (requestHandler.IsCompleted(storageRequestId)) {
            RequestHandlersByStorageRequestId.erase(storageRequestId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template void TDirectBlockGroup::HandleReadResult<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult& response);

template void TDirectBlockGroup::HandleReadResult<NKikimrBlobStorage::NDDisk::TEvReadResult>(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvReadResult& response);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
