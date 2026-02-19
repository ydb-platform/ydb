#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;


namespace {

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TDirectBlockGroup::TPendingRequests
{
    explicit TPendingRequests(ui32 responsesExpected)
        : ResponsesExpected(responsesExpected)
    {}

    ui32 ResponsesHandled = 0;
    const ui32 ResponsesExpected;
};

class TDirectBlockGroup::TDirtyMap
{
private:
    // TODO позже удалить данные при flush'е
    TVector<TBlockMeta> BlocksMeta;

public:
    TDirtyMap(ui64 blocksCount, size_t numberOfPersistentBuffers)
        : BlocksMeta(blocksCount, TBlockMeta(numberOfPersistentBuffers))
    {}

    ui64 GetLsnByPersistentBufferIndex(ui64 blockIndex,
                                       ui64 persistBufferIndex);
    void SetLsnByPersistentBufferIndex(ui64 blockIndex, ui64 persistBufferIndex,
                                       ui64 lsn);
    bool IsBlockWritten(ui64 blockIndex) const;
    bool IsBlockFlushedToDDisk(ui64 blockIndex) const;
    void OnBlockWriteCompleted(
        ui64 blockIndex,
        const TWriteRequestHandler::TPersistentBufferWriteMeta& writeMeta);
    void OnBlockFlushCompleted(ui64 blockIndex, ui64 persistBufferIndex,
                               ui64 lsn);
};

ui64 TDirectBlockGroup::TDirtyMap::GetLsnByPersistentBufferIndex(
    ui64 blockIndex, ui64 persistBufferIndex)
{
    return BlocksMeta[blockIndex].LsnByPersistentBufferIndex[persistBufferIndex];
}

void TDirectBlockGroup::TDirtyMap::SetLsnByPersistentBufferIndex(
    ui64 blockIndex, ui64 persistBufferIndex, ui64 lsn)
{
    BlocksMeta[blockIndex].LsnByPersistentBufferIndex[persistBufferIndex] = lsn;
}

bool TDirectBlockGroup::TDirtyMap::IsBlockWritten(ui64 blockIndex) const
{
    return BlocksMeta[blockIndex].IsWritten();
}

bool TDirectBlockGroup::TDirtyMap::IsBlockFlushedToDDisk(ui64 blockIndex) const
{
    return BlocksMeta[blockIndex].IsFlushedToDDisk();
}

void TDirectBlockGroup::TDirtyMap::OnBlockWriteCompleted(
    ui64 blockIndex,
    const TWriteRequestHandler::TPersistentBufferWriteMeta& writeMeta)
{
    BlocksMeta[blockIndex].OnWriteCompleted(writeMeta);
}

void TDirectBlockGroup::TDirtyMap::OnBlockFlushCompleted(
    ui64 blockIndex, ui64 persistBufferIndex, ui64 lsn)
{
    BlocksMeta[blockIndex].OnFlushCompleted(persistBufferIndex, lsn);
}

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
    , StorageTransport(
          std::make_unique<NTransport::TICStorageTransport>(actorSystem))
{
    auto guard = Guard(Lock);

    Y_UNUSED(TabletId);
    Y_UNUSED(Generation);
    Y_UNUSED(BlockSize);
    Y_UNUSED(BlocksCount);
    Y_UNUSED(StorageRequestId);

    DirtyMap = std::make_unique<TDirtyMap>(TDirtyMap(BlocksCount, persistentBufferDDiskIds.size()));

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

TDirectBlockGroup::~TDirectBlockGroup() = default;

void TDirectBlockGroup::EstablishConnections()
{
    auto connectionsPending = std::make_shared<TPendingRequests>(
        PersistentBufferConnections.size());
    auto guard = Guard(Lock);

    for (size_t i = 0; i < PersistentBufferConnections.size(); i++) {
        auto future = StorageTransport->Connect(
            PersistentBufferConnections[i].GetServiceId(),
            PersistentBufferConnections[i].Credentials);

        future.Subscribe(
            [weakSelf = weak_from_this(), i,
             connectionsPending = connectionsPending](
                const TFuture<NKikimrBlobStorage::NDDisk::TEvConnectResult>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->HandlePersistentBufferConnected(i, f.GetValue(),
                                                          connectionsPending);
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
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result,
    std::shared_ptr<TPendingRequests> connectionsPending)
{
    auto guard = Guard(Lock);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        PersistentBufferConnections[index].Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();

        connectionsPending->ResponsesHandled += 1;
    } else {
        Y_ABORT("TDirectBlockGroup::HandlePersistentBufferConnected: connection failed - unhandled error");
    }

    bool allConnectionsEstablised =
        connectionsPending->ResponsesHandled ==
        connectionsPending->ResponsesExpected;
    if (allConnectionsEstablised) {
        RestoreFromPersistentBuffer();
    }
}

// TODO заблокировать IO до полного восстановления (где-то раньше)
void TDirectBlockGroup::RestoreFromPersistentBuffer()
{
    size_t numberOfRequests = PersistentBufferConnections.size();
    auto requestsPending =
        std::make_shared<TPendingRequests>(numberOfRequests);
    for (size_t i = 0; i < numberOfRequests; i++) {
        const auto& ddiskConnection = PersistentBufferConnections[i];
        ++StorageRequestId;

        auto future = StorageTransport->ListPersistentBuffer(
            ddiskConnection.GetServiceId(), ddiskConnection.Credentials,
            StorageRequestId);

        future.Subscribe(
            [this, requestId = StorageRequestId,
             requestsPending = requestsPending,
             persistentBufferIndex = i](const auto& f)
            {
                const auto& result = f.GetValue();
                HandleListPersistentBufferResultOnRestore(
                    requestId, result, persistentBufferIndex, requestsPending);
            });
    }
}

void TDirectBlockGroup::HandleListPersistentBufferResultOnRestore(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& result,
    size_t persistentBufferIndex,
    std::shared_ptr<TPendingRequests> requestsPending)
{
    Y_UNUSED(storageRequestId);
    ++requestsPending->ResponsesHandled;

    Y_ABORT_UNLESS(result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

    // Despite the fact, that this method can be invoked from different threads, we don't need locks.
    // The reason is each thread accesses strictly own its data and doesn't modify common memory.

    const auto& records = result.GetRecords();
    for (const auto& record: records) {
        const NKikimrBlobStorage::NDDisk::TBlockSelector& selector = record.GetSelector();
        const size_t startIndex = selector.GetOffsetInBytes() / BlockSize;
        const size_t blocksNumber = selector.GetSize() / BlockSize;

        for (size_t i = startIndex; i < startIndex + blocksNumber; ++i) {
            auto lsn = DirtyMap->GetLsnByPersistentBufferIndex(
                i, persistentBufferIndex);
            lsn = std::max(lsn, record.GetLsn());
            DirtyMap->SetLsnByPersistentBufferIndex(i, persistentBufferIndex,
                                                    lsn);
        }
    }

    if (requestsPending->ResponsesHandled == requestsPending->ResponsesExpected) {
        RestoreFromPersistentBufferFinised(); // finish actor bootstrap
    }
}

void TDirectBlockGroup::RestoreFromPersistentBufferFinised()
{
    // function's body will be writen a bit later
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

    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.PBWrite.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    for (size_t i = 0; i < 3; i++) {
        execSpan.Event("PB request start");
        const auto& ddiskConnection = PersistentBufferConnections[i];
        ++StorageRequestId;

        auto& childSpan = requestHandler->GetChildSpan(StorageRequestId, i);
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
            childSpan);

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

        execSpan.Event("PB request end");
    }

    execSpan.EndOk();

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::HandleWritePersistentBufferResult(
    std::shared_ptr<TWriteRequestHandler> requestHandler,
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult& result)
{
    auto guard = Guard(Lock);

    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.HandlePBWriteResult.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->ChildSpanEndOk(storageRequestId);

        if (requestHandler->IsCompleted(storageRequestId)) {
            // TODO fix this. blockMeta is different for each meta in writesMeta
            execSpan.Event("Start update meta");
            const auto& writesMeta = requestHandler->GetWritesMeta();
            for (const auto& meta: writesMeta) {
                DirtyMap->OnBlockWriteCompleted(requestHandler->GetStartIndex(), meta);
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
    for (size_t i = 0; i < 3; i++) {
        auto syncRequestHandler = std::make_shared<TSyncRequestHandler>(
            ActorSystem,
            requestHandler.GetStartIndex(),
            i,   // persistentBufferIndex
            DirtyMap->GetLsnByPersistentBufferIndex(requestHandler.GetStartIndex(), i),
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
        syncRequestHandler->Span);

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

    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.HandleSyncWithPersistentBufferResult.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        DirtyMap->OnBlockFlushCompleted(
            requestHandler->GetStartIndex(),
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
        eraseRequestHandler->Span);

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

    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.ReadBlocks.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto startIndex = requestHandler->GetStartIndex();

    // Block is not written
    if (!DirtyMap->IsBlockWritten(startIndex)) {
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
        return requestHandler->GetFuture();
    }

    ++StorageRequestId;

    if (!DirtyMap->IsBlockFlushedToDDisk(startIndex)) {
        const auto& ddiskConnection = PersistentBufferConnections[0];

        auto& childSpan = requestHandler->GetChildSpan(StorageRequestId, true);
        auto future = StorageTransport->ReadPersistentBuffer(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                0,   // vChunkIndex
                requestHandler->GetStartOffset(),
                requestHandler->GetSize()),
            DirtyMap->GetLsnByPersistentBufferIndex(startIndex, 0),
            NKikimr::NDDisk::TReadInstruction(true),
            requestHandler->GetData(),
            childSpan);

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

        auto& childSpan = requestHandler->GetChildSpan(StorageRequestId, false);
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

    execSpan.EndOk();

    return requestHandler->GetFuture();
}

template <typename TEvent>
void TDirectBlockGroup::HandleReadResult(
    std::shared_ptr<TReadRequestHandler> requestHandler,
    ui64 storageRequestId,
    const TEvent& result)
{
    auto guard = Guard(Lock);

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
