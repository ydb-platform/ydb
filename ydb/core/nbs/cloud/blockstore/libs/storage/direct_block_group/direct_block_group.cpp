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
    : TabletId(tabletId)
    , Generation(generation)
    , BlockSize(blockSize)
    , BlocksCount(blocksCount)
    , BlocksMeta(BlocksCount, TBlockMeta(persistentBufferDDiskIds.size()))
    , StorageTransport(std::make_unique<TICStorageTransport>())
{
    auto guard = Guard(Lock);

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
    auto guard = Guard(Lock);

    auto sendConnectRequests = [&](const TVector<TDDiskConnection>& connections, ui64 startRequestId = 0) {
        for (size_t i = 0; i < connections.size(); i++) {
            auto future = StorageTransport->Connect(
                connections[i].GetServiceId(),
                connections[i].Credentials,
                startRequestId + i);

            future.Subscribe([this, requestId = startRequestId + i](const auto& f) {
                HandleConnectResult(requestId, f.GetValue());
            });
        }
    };

    sendConnectRequests(PersistentBufferConnections);
    // Send connect requests to ddisks with offset by persistent buffer connections count
    sendConnectRequests(DDiskConnections, PersistentBufferConnections.size());
}

void TDirectBlockGroup::HandleConnectResult(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
    auto guard = Guard(Lock);

    TDDiskConnection* ddiskConnection = nullptr;
    if (storageRequestId < PersistentBufferConnections.size()) {
        ddiskConnection = &PersistentBufferConnections[storageRequestId];
    } else {
        ddiskConnection = &DDiskConnections[storageRequestId - PersistentBufferConnections.size()];
    }

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        ddiskConnection->Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
    }
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TWriteBlocksLocalResponse> TDirectBlockGroup::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto guard = Guard(Lock);

    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TWriteRequestHandler>(std::move(request), std::move(traceId), TabletId);

    for (size_t i = 0; i < 3; i++) {
        const auto& ddiskConnection = PersistentBufferConnections[i];
        ++StorageRequestId;

        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "WriteBlocksLocal" << " requestId# " << StorageRequestId);

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
            requestHandler->GetChildSpan(StorageRequestId, i),
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
    auto guard = Guard(Lock);

    // That means that request is already completed
    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TWriteRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler.ChildSpanEndOk(storageRequestId);

        if (requestHandler.IsCompleted(storageRequestId)) {
            auto& blockMeta = BlocksMeta[requestHandler.GetStartIndex()];
            const auto& writesMeta = requestHandler.GetWritesMeta();
            for (const auto& meta : writesMeta) {
                blockMeta.OnWriteCompleted(meta);
            }

            RequestBlockFlush(requestHandler);
            requestHandler.SetResponse();

            requestHandler.Span.EndOk();
            if (WriteBlocksReplyCallback) {
                WriteBlocksReplyCallback(true);
            }

            RequestHandlersByStorageRequestId.erase(storageRequestId);
        }
    } else {
        // TODO: add error handling
        requestHandler.ChildSpanEndError(storageRequestId, "HandleWritePersistentBufferResult failed");
        requestHandler.Span.EndError("HandleWritePersistentBufferResult failed");

        if (WriteBlocksReplyCallback) {
            WriteBlocksReplyCallback(result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDirectBlockGroup::RequestBlockFlush(
    TWriteRequestHandler& requestHandler)
{
    auto guard = Guard(Lock);

    const auto& blockMeta = BlocksMeta[requestHandler.GetStartIndex()];

    for (size_t i = 0; i < 3; i++) {
        auto syncRequestHandler = std::make_shared<TSyncRequestHandler>(
            requestHandler.GetStartIndex(),
            i, // persistentBufferIndex
            blockMeta.LsnByPersistentBufferIndex[i],
            requestHandler.Span.GetTraceId(),
            TabletId);

        SyncQueue.push(syncRequestHandler);
    }

    ProcessSyncQueue();
}

void TDirectBlockGroup::ProcessSyncQueue()
{
    if (!SyncQueue.empty()) {
        const auto& syncRequestHandler = SyncQueue.front();
        auto persistentBufferIndex = syncRequestHandler->GetPersistentBufferIndex();
        const auto& ddiskConnection = DDiskConnections[persistentBufferIndex];
        const auto& persistentBufferConnection = PersistentBufferConnections[persistentBufferIndex];

        ++StorageRequestId;

        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "ProcessSyncQueue" << " requestId# " << StorageRequestId);

        auto future = StorageTransport->SyncWithPersistentBuffer(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                syncRequestHandler->GetStartOffset(),
                syncRequestHandler->GetSize()),
            syncRequestHandler->GetLsn(),
            std::make_tuple(
                persistentBufferConnection.DDiskId.NodeId,
                persistentBufferConnection.DDiskId.PDiskId,
                persistentBufferConnection.DDiskId.DDiskSlotId),
            persistentBufferConnection.Credentials.DDiskInstanceGuid.value(),
            syncRequestHandler->Span.GetTraceId(),
            StorageRequestId);

        future.Subscribe([this, requestId = StorageRequestId](const auto& f) {
            const auto& result = f.GetValue();
            HandleSyncWithPersistentBufferResult(requestId, result);
        });

        RequestHandlersByStorageRequestId[StorageRequestId] = syncRequestHandler;
        SyncQueue.pop();
    }
}

void TDirectBlockGroup::HandleSyncWithPersistentBufferResult(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult& result)
{
    auto guard = Guard(Lock);

    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TSyncRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        BlocksMeta[requestHandler.GetStartIndex()].OnFlushCompleted(
            requestHandler.GetPersistentBufferIndex(),
            requestHandler.GetLsn());

        requestHandler.Span.EndOk();

        ProcessSyncQueue();

        RequestBlockErase(requestHandler);

        RequestHandlersByStorageRequestId.erase(storageRequestId);
    } else {
        // TODO: add error handling
        requestHandler.Span.EndError("HandleSyncResult failed");
    }
}

void TDirectBlockGroup::RequestBlockErase(
    TSyncRequestHandler& requestHandler)
{
    auto eraseRequestHandler = std::make_shared<TEraseRequestHandler>(
        requestHandler.GetStartIndex(),
        requestHandler.GetPersistentBufferIndex(),
        requestHandler.GetLsn(),
        requestHandler.Span.GetTraceId(),
        TabletId);

    ++StorageRequestId;

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "RequestBlockErase" << " requestId# " << StorageRequestId);

    auto future = StorageTransport->ErasePersistentBuffer(
        PersistentBufferConnections[requestHandler.GetPersistentBufferIndex()]
            .GetServiceId(),
        PersistentBufferConnections[requestHandler.GetPersistentBufferIndex()]
            .Credentials,
        NKikimr::NDDisk::TBlockSelector(
            0,   // vChunkIndex
            eraseRequestHandler->GetStartOffset(),
            eraseRequestHandler->GetSize()),
        eraseRequestHandler->GetLsn(),
        eraseRequestHandler->Span.GetTraceId(),
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
    auto guard = Guard(Lock);

    // That means that request is already completed
    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TEraseRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler.Span.EndOk();

        RequestHandlersByStorageRequestId.erase(storageRequestId);
    } else {
        // TODO: add error handling
        requestHandler.Span.EndError("HandleErasePersistentBufferResult failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse> TDirectBlockGroup::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto guard = Guard(Lock);

    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TReadRequestHandler>(std::move(request), std::move(traceId), TabletId);

    auto startIndex = requestHandler->GetStartIndex();

    // Block is not writed
    if (!BlocksMeta[startIndex].IsWritten())
    {
        requestHandler->Span.EndOk();
        if (ReadBlocksReplyCallback) {
            ReadBlocksReplyCallback(true);
        }

        auto promise = NThreading::NewPromise<TReadBlocksLocalResponse>();

        promise.SetValue(TReadBlocksLocalResponse());

        return promise.GetFuture();
    }

    ++StorageRequestId;

    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "ReadBlocksLocal" << " requestId# " << StorageRequestId);

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
            requestHandler->GetChildSpan(StorageRequestId, true),
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
            requestHandler->GetChildSpan(StorageRequestId, false),
            StorageRequestId);

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
    auto guard = Guard(Lock);

    // That means that request is already completed
    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TReadRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler.ChildSpanEndOk(storageRequestId);

        if (requestHandler.IsCompleted(storageRequestId)) {
            requestHandler.SetResponse();

            requestHandler.Span.EndOk();
            if (ReadBlocksReplyCallback) {
                ReadBlocksReplyCallback(true);
            }

            RequestHandlersByStorageRequestId.erase(storageRequestId);
        }
    } else {
        // TODO: add error handling
        requestHandler.ChildSpanEndError(storageRequestId, "HandleReadResult failed");
        requestHandler.Span.EndError("HandleReadResult failed");

        if (ReadBlocksReplyCallback) {
            ReadBlocksReplyCallback(false);
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
