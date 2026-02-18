#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    NActors::TActorSystem* actorSystem,
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddisksIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds,
    ui32 blockSize,
    ui64 blocksCount)
    : ActorSystem(actorSystem)
    , TabletId(tabletId)
    , Generation(generation)
    , BlockSize(blockSize)
    , BlocksCount(blocksCount)
    , BlocksMeta(BlocksCount, TBlockMeta(persistentBufferDDiskIds.size()))
    , StorageTransport(
          std::make_unique<NTransport::TICStorageTransport>(actorSystem))
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

    // Now we assume that ddisksIds and persistentBufferDDiskIds have the same
    // size since we flush each persistent buffer to ddisk with the same index
    Y_ABORT_UNLESS(ddisksIds.size() == persistentBufferDDiskIds.size());

    addDDiskConnections(std::move(ddisksIds), DDiskConnections, false);
    addDDiskConnections(
        std::move(persistentBufferDDiskIds),
        PersistentBufferConnections,
        true);
}

void TDirectBlockGroup::EstablishConnections()
{
    auto guard = Guard(Lock);

    for (size_t i = 0; i < PersistentBufferConnections.size(); i++) {
        auto future = StorageTransport->Connect(
            PersistentBufferConnections[i].GetServiceId(),
            PersistentBufferConnections[i].Credentials);

        future.Subscribe(
            [weakSelf = weak_from_this(),
             i](const TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->HandlePersistentBufferConnected(i, f.GetValue());
                }
            });
    }

    for (size_t i = 0; i < DDiskConnections.size(); i++) {
        auto future = StorageTransport->Connect(
            DDiskConnections[i].GetServiceId(),
            DDiskConnections[i].Credentials);

        future.Subscribe(
            [weakSelf = weak_from_this(),
             i](const TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->HandleDDiskBufferConnected(i, f.GetValue());
                }
            });
    }
}

void TDirectBlockGroup::HandlePersistentBufferConnected(
    size_t index,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
    auto guard = Guard(Lock);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        PersistentBufferConnections[index].Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
    }
}

void TDirectBlockGroup::HandleDDiskBufferConnected(
    size_t index,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
    auto guard = Guard(Lock);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        DDiskConnections[index].Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
    }
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TWriteBlocksLocalResponse>
TDirectBlockGroup::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    using TEvWritePersistentBufferResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>;

    auto guard = Guard(Lock);

    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TWriteRequestHandler>(
        ActorSystem,
        std::move(request),
        std::move(traceId),
        TabletId);

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
            requestHandler->GetChildSpan(StorageRequestId, i));

        future.Subscribe(
            [weakSelf = weak_from_this(),
             storageRequestId = StorageRequestId,
             requestHandler](const TEvWritePersistentBufferResultFuture& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->HandleWritePersistentBufferResult(
                        requestHandler,
                        storageRequestId,
                        f.GetValue());
                }
            });

        requestHandler->OnWriteRequested(
            StorageRequestId,
            i,   // persistentBufferIndex
            StorageRequestId);
    }

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::HandleWritePersistentBufferResult(
    std::shared_ptr<TWriteRequestHandler> requestHandler,
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult& result)
{
    auto guard = Guard(Lock);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->ChildSpanEndOk(storageRequestId);

        if (requestHandler->IsCompleted(storageRequestId)) {
            auto& blockMeta = BlocksMeta[requestHandler->GetStartIndex()];
            const auto& writesMeta = requestHandler->GetWritesMeta();
            for (const auto& meta: writesMeta) {
                blockMeta.OnWriteCompleted(meta);
            }

            RequestBlockFlush(*requestHandler);
            requestHandler->SetResponse(MakeError(S_OK));

            requestHandler->Span.EndOk();
        }
    } else {
        // TODO: add error handling
        requestHandler->ChildSpanEndError(
            storageRequestId,
            "HandleWritePersistentBufferResult failed");
        requestHandler->Span.EndError(
            "HandleWritePersistentBufferResult failed");

        requestHandler->SetResponse(MakeError(E_FAIL, result.GetErrorReason()));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDirectBlockGroup::RequestBlockFlush(
    const TWriteRequestHandler& requestHandler)
{
    const auto& blockMeta = BlocksMeta[requestHandler.GetStartIndex()];

    for (size_t i = 0; i < 3; i++) {
        auto syncRequestHandler = std::make_shared<TSyncRequestHandler>(
            ActorSystem,
            requestHandler.GetStartIndex(),
            i,   // persistentBufferIndex
            blockMeta.LsnByPersistentBufferIndex[i],
            requestHandler.Span.GetTraceId(),
            TabletId);

        SyncQueue.push(syncRequestHandler);
    }

    ProcessSyncQueue();
}

void TDirectBlockGroup::ProcessSyncQueue()
{
    using TEvSyncWithPersistentBufferResultFuture = NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult>;

    if (SyncQueue.empty()) {
        return;
    }

    auto syncRequestHandler = SyncQueue.front();
    SyncQueue.pop();

    auto persistentBufferIndex = syncRequestHandler->GetPersistentBufferIndex();
    const auto& ddiskConnection = DDiskConnections[persistentBufferIndex];
    const auto& persistentBufferConnection =
        PersistentBufferConnections[persistentBufferIndex];

    ++StorageRequestId;

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
        syncRequestHandler->Span.GetTraceId());

    future.Subscribe(
        [weakSelf = weak_from_this(),
         syncRequestHandler](const TEvSyncWithPersistentBufferResultFuture& f)
        {
            if (auto self = weakSelf.lock()) {
                self->HandleSyncWithPersistentBufferResult(
                    syncRequestHandler,
                    f.GetValue());
            }
        });
}

void TDirectBlockGroup::HandleSyncWithPersistentBufferResult(
    std::shared_ptr<TSyncRequestHandler> requestHandler,
    const NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult& result)
{
    auto guard = Guard(Lock);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        BlocksMeta[requestHandler->GetStartIndex()].OnFlushCompleted(
            requestHandler->GetPersistentBufferIndex(),
            requestHandler->GetLsn());

        requestHandler->Span.EndOk();

        ProcessSyncQueue();

        RequestBlockErase(*requestHandler);
    } else {
        // TODO: add error handling
        requestHandler->Span.EndError("HandleSyncResult failed");
    }
}

void TDirectBlockGroup::RequestBlockErase(
    const TSyncRequestHandler& requestHandler)
{
    using TEvErasePersistentBufferResultFuture = NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult>;

    auto eraseRequestHandler = std::make_shared<TEraseRequestHandler>(
        ActorSystem,
        requestHandler.GetStartIndex(),
        requestHandler.GetPersistentBufferIndex(),
        requestHandler.GetLsn(),
        requestHandler.Span.GetTraceId(),
        TabletId);

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
        eraseRequestHandler->GetLsn(),
        eraseRequestHandler->Span.GetTraceId());

    future.Subscribe(
        [weakSelf = weak_from_this(),
         eraseRequestHandler](const TEvErasePersistentBufferResultFuture& f)
        {
            const auto& result = f.GetValue();
            if (auto self = weakSelf.lock()) {
                self->HandleErasePersistentBufferResult(
                    std::move(eraseRequestHandler),
                    result);
            }
        });
}

void TDirectBlockGroup::HandleErasePersistentBufferResult(
    std::shared_ptr<TEraseRequestHandler> requestHandler,
    const NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult& result)
{
    auto guard = Guard(Lock);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->Span.EndOk();
    } else {
        // TODO: add error handling
        requestHandler->Span.EndError(
            "HandleErasePersistentBufferResult failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse>
TDirectBlockGroup::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    using TEvReadResultFuture =
        NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult>;
    using TEvReadPersistentBufferResultFeature = NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>;

    auto guard = Guard(Lock);

    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TReadRequestHandler>(
        ActorSystem,
        std::move(request),
        std::move(traceId),
        TabletId);

    auto startIndex = requestHandler->GetStartIndex();

    // Block is not written
    if (!BlocksMeta[startIndex].IsWritten()) {
        auto data = requestHandler->GetData();
        if (auto guard = data.Acquire()) {
            const auto& sglist = guard.Get();
            const auto& block = sglist[0];
            memset(const_cast<char*>(block.Data()), 0, block.Size());
        } else {
            Y_ABORT_UNLESS(false);
        }

        requestHandler->SetResponse(MakeError(S_OK));

        requestHandler->Span.EndOk();
        return requestHandler->GetFuture();
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
            requestHandler->GetChildSpan(StorageRequestId, true));

        future.Subscribe(
            [weakSelf = weak_from_this(),
             requestHandler,
             requestId = StorageRequestId](
                const TEvReadPersistentBufferResultFeature& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->HandleReadResult(
                        requestHandler,
                        requestId,
                        f.GetValue());
                }
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
            requestHandler->GetChildSpan(StorageRequestId, false));

        future.Subscribe(
            [weakSelf = weak_from_this(),
             requestHandler,
             requestId = StorageRequestId](const TEvReadResultFuture& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->HandleReadResult(
                        requestHandler,
                        requestId,
                        f.GetValue());
                }
            });
    }

    return requestHandler->GetFuture();
}

template <typename TEvent>
void TDirectBlockGroup::HandleReadResult(
    std::shared_ptr<TReadRequestHandler> requestHandler,
    ui64 storageRequestId,
    const TEvent& result)
{
    auto guard = Guard(Lock);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->ChildSpanEndOk(storageRequestId);

        if (requestHandler->IsCompleted(storageRequestId)) {
            requestHandler->SetResponse(MakeError(S_OK));

            requestHandler->Span.EndOk();
        }
    } else {
        // TODO: add error handling
        requestHandler->ChildSpanEndError(
            storageRequestId,
            "HandleReadResult failed");
        requestHandler->Span.EndError("HandleReadResult failed");

        requestHandler->SetResponse(MakeError(E_FAIL, result.GetErrorReason()));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
