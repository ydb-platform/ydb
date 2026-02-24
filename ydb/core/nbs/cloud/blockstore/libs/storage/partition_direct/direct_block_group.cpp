#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

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
    Executor = TExecutor::Create("DirectBlockGroup");
    Executor->Start();

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

TDirectBlockGroup::~TDirectBlockGroup()
{
    if (Executor) {
        Executor->Stop();
    }
}

void TDirectBlockGroup::EstablishConnections()
{
    for (size_t i = 0; i < PersistentBufferConnections.size(); i++) {
        Executor->ExecuteSimple(
            [weakSelf = weak_from_this(), i]()
            {
                if (auto self = weakSelf.lock()) {
                    self->DoEstablishPersistentBufferConnection(i);
                }
            });
    }

    for (size_t i = 0; i < DDiskConnections.size(); i++) {
        Executor->ExecuteSimple(
            [weakSelf = weak_from_this(), i]()
            {
                if (auto self = weakSelf.lock()) {
                    self->DoEstablishDDiskConnection(i);
                }
            });
    }
}

void TDirectBlockGroup::DoEstablishPersistentBufferConnection(size_t i)
{
    auto future = StorageTransport->Connect(
        PersistentBufferConnections[i].GetServiceId(),
        PersistentBufferConnections[i].Credentials);

    const auto& resultOrError = Executor->ResultOrError(std::move(future));
    if (!HasError(resultOrError)) {
        HandlePersistentBufferConnected(i, resultOrError.GetResult());
    }
    // TODO: add error handling
}

void TDirectBlockGroup::DoEstablishDDiskConnection(size_t i)
{
    auto future = StorageTransport->Connect(
        DDiskConnections[i].GetServiceId(),
        DDiskConnections[i].Credentials);

    const auto& resultOrError = Executor->ResultOrError(std::move(future));
    if (!HasError(resultOrError)) {
        HandleDDiskBufferConnected(i, resultOrError.GetResult());
    }
    // TODO: add error handling
}

void TDirectBlockGroup::HandlePersistentBufferConnected(
    size_t index,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        PersistentBufferConnections[index].Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
    }
}

void TDirectBlockGroup::HandleDDiskBufferConnected(
    size_t index,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
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
    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TWriteRequestHandler>(
        ActorSystem,
        std::move(request),
        std::move(traceId),
        TabletId);

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(), requestHandler]()
        {
            if (auto self = weakSelf.lock()) {
                self->DoWriteBlocksLocal(requestHandler);
            }
        });

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::DoWriteBlocksLocal(
    std::shared_ptr<TWriteRequestHandler> requestHandler)
{
    using TEvWritePersistentBufferResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>;

    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.PBWrite.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    TVector<TEvWritePersistentBufferResultFuture> futures;
    TVector<ui64> storageRequestIds;
    futures.reserve(3);
    storageRequestIds.reserve(3);

    for (size_t i = 0; i < 3; i++) {
        execSpan.Event("PB request start");
        const ui64 storageRequestId = ++StorageRequestId;
        const auto& ddiskConnection = PersistentBufferConnections[i];

        auto& childSpan = requestHandler->GetChildSpan(storageRequestId, i);
        auto future = StorageTransport->WritePersistentBuffer(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                requestHandler->GetStartOffset(),
                requestHandler->GetSize()),
            storageRequestId,   // lsn
            NKikimr::NDDisk::TWriteInstruction(0),
            requestHandler->GetData(),
            childSpan);

        requestHandler->OnWriteRequested(storageRequestId, i, storageRequestId);

        execSpan.Event("PB request end");

        futures.push_back(std::move(future));
        storageRequestIds.push_back(storageRequestId);
    }

    execSpan.EndOk();

    for (size_t i = 0; i < 3; i++) {
        const auto& resultOrError =
            Executor->ResultOrError(std::move(futures[i]));

        if (!HasError(resultOrError)) {
            HandleWritePersistentBufferResult(
                requestHandler,
                storageRequestIds[i],
                resultOrError.GetResult());
        }
        // TODO: add error handling
    }
}

void TDirectBlockGroup::HandleWritePersistentBufferResult(
    std::shared_ptr<TWriteRequestHandler> requestHandler,
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult& result)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.HandlePBWriteResult.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->ChildSpanEndOk(storageRequestId);

        if (requestHandler->IsCompleted(storageRequestId)) {
            auto& blockMeta = BlocksMeta[requestHandler->GetStartIndex()];
            execSpan.Event("Start update meta");
            const auto& writesMeta = requestHandler->GetWritesMeta();
            for (const auto& meta: writesMeta) {
                blockMeta.OnWriteCompleted(meta);
            }

            execSpan.Event("Start RequestBlockFlush");
            RequestBlockFlush(*requestHandler);
            execSpan.Event("Start SetResponse");
            requestHandler->SetResponse(MakeError(S_OK));
            execSpan.Event("Finish SetResponse");

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

    execSpan.EndOk();
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
    if (SyncQueue.empty()) {
        return;
    }

    auto syncRequestHandler = SyncQueue.front();
    SyncQueue.pop();

    auto persistentBufferIndex = syncRequestHandler->GetPersistentBufferIndex();
    const auto& ddiskConnection = DDiskConnections[persistentBufferIndex];
    const auto& persistentBufferConnection =
        PersistentBufferConnections[persistentBufferIndex];

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
        syncRequestHandler->Span);

    const auto& resultOrError =
        Executor->ResultOrError(std::move(future));
    if (!HasError(resultOrError)) {
        HandleSyncWithPersistentBufferResult(
            std::move(syncRequestHandler),
            resultOrError.GetResult());
    }
    // TODO: add error handling
}

void TDirectBlockGroup::HandleSyncWithPersistentBufferResult(
    std::shared_ptr<TSyncRequestHandler> requestHandler,
    const NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult& result)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.HandleSyncWithPersistentBufferResult.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

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

    execSpan.EndOk();
}

void TDirectBlockGroup::RequestBlockErase(
    const TSyncRequestHandler& requestHandler)
{
    auto eraseRequestHandler = std::make_shared<TEraseRequestHandler>(
        ActorSystem,
        requestHandler.GetStartIndex(),
        requestHandler.GetPersistentBufferIndex(),
        requestHandler.GetLsn(),
        requestHandler.Span.GetTraceId(),
        TabletId);

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
        eraseRequestHandler->Span);


    const auto& resultOrError = Executor->ResultOrError(std::move(future));
    if (!HasError(resultOrError)) {
        HandleErasePersistentBufferResult(
            std::move(eraseRequestHandler),
            resultOrError.GetResult());
    }
    // TODO: add error handling
}

void TDirectBlockGroup::HandleErasePersistentBufferResult(
    std::shared_ptr<TEraseRequestHandler> requestHandler,
    const NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult& result)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.HandleErasePersistentBufferResult.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->Span.EndOk();
    } else {
        // TODO: add error handling
        requestHandler->Span.EndError(
            "HandleErasePersistentBufferResult failed");
    }

    execSpan.EndOk();
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse>
TDirectBlockGroup::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TReadRequestHandler>(
        ActorSystem,
        std::move(request),
        std::move(traceId),
        TabletId);

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(), requestHandler]()
        {
            if (auto self = weakSelf.lock()) {
                self->DoReadBlocksLocal(requestHandler);
            }
        });

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::DoReadBlocksLocal(
    std::shared_ptr<TReadRequestHandler> requestHandler)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.ReadBlocks.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

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
        execSpan.EndOk();
        return;
    }

    const ui64 storageRequestId = ++StorageRequestId;

    if (!BlocksMeta[startIndex].IsFlushedToDDisk()) {
        const auto& ddiskConnection = PersistentBufferConnections[0];

        auto& childSpan = requestHandler->GetChildSpan(storageRequestId, true);
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
            childSpan);

        execSpan.EndOk();

        const auto& resultOrError = Executor->ResultOrError(std::move(future));
        if (!HasError(resultOrError)) {
            HandleReadResult(
                std::move(requestHandler),
                storageRequestId,
                resultOrError.GetResult());
        }
        // TODO: add error handling
    } else {
        const auto& ddiskConnection = DDiskConnections[0];

        auto& childSpan = requestHandler->GetChildSpan(storageRequestId, false);
        auto future = StorageTransport->Read(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                requestHandler->GetStartOffset(),
                requestHandler->GetSize()),
            NKikimr::NDDisk::TReadInstruction(true),
            requestHandler->GetData(),
            childSpan);

        execSpan.EndOk();

        const auto& resultOrError = Executor->ResultOrError(std::move(future));
        if (!HasError(resultOrError)) {
            HandleReadResult(
                std::move(requestHandler),
                storageRequestId,
                resultOrError.GetResult());
        }
        // TODO: add error handling
    }
}

template <typename TEvent>
void TDirectBlockGroup::HandleReadResult(
    std::shared_ptr<TReadRequestHandler> requestHandler,
    ui64 storageRequestId,
    const TEvent& result)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.ReadBlocks.HandleReadResult.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

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

    execSpan.EndOk();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
