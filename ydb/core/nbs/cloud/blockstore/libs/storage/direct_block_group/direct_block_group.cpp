#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/fast_path_service/storage_transport/ic_storage_transport.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

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
    , StorageTransport(std::make_unique<TICStorageTransport>())
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

    // Now we assume that ddisksIds and persistentBufferDDiskIds have the same size
    // since we flush each persistent buffer to ddisk with the same index
    Y_ABORT_UNLESS(ddisksIds.size() == persistentBufferDDiskIds.size());

    addDDiskConnections(std::move(ddisksIds), DDiskConnections, false);
    addDDiskConnections(std::move(persistentBufferDDiskIds), PersistentBufferConnections, true);
    LOG_DEBUG_S(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "end of TDirectBlockGroup::TDirectBlockGroup: "
            << "PersistentBufferConnections.size() == " << PersistentBufferConnections.size());


}

TDirectBlockGroup::~TDirectBlockGroup() = default;

void TDirectBlockGroup::EstablishConnections()
{
    auto guard = Guard(Lock);

    auto connectionsStalking = std::make_shared<TRequestsStalking>(
        DDiskConnections.size() + PersistentBufferConnections.size());
    auto sendConnectRequests =
        [&, connectionsStalking = connectionsStalking](
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
                 connectionsStalking = connectionsStalking](
                    const auto& f)
                {
                    HandleConnectResult(requestId, f.GetValue(),
                                        connectionsStalking);
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
    std::shared_ptr<TRequestsStalking> connectionsStalking)
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
        connectionsStalking->ResponsesHandled += 1;
    } else {
        Y_ABORT("TDirectBlockGroup::HandleConnectResult: connection failed - unhandled error");
    }

    bool allConnectionsEstablised =
        connectionsStalking->ResponsesHandled ==
        connectionsStalking->ResponsesExpected;
    if (allConnectionsEstablised) {
        RestorePersistentBuffer();
    }
}

// TODO заблокировать IO до полного восстановления (где-то раньше)
void TDirectBlockGroup::RestorePersistentBuffer()
{
    LOG_DEBUG_S(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "RestorePersistentBuffer started"
            << "PersistentBufferConnections.size(): " << PersistentBufferConnections.size());
    size_t numberOfRequests = PersistentBufferConnections.size();
    auto requestsStalking =
        std::make_shared<TRequestsStalking>(numberOfRequests);
    for (size_t i = 0; i < numberOfRequests; i++) {
        const auto& ddiskConnection = PersistentBufferConnections[i];
        ++StorageRequestId;

        auto future = StorageTransport->ListPersistentBuffer(
            ddiskConnection.GetServiceId(), ddiskConnection.Credentials,
            StorageRequestId);

        future.Subscribe(
            [this, requestId = StorageRequestId,
             requestsStalking = requestsStalking,
             persistentBufferIndex = i](const auto& f)
            {
                LOG_DEBUG_S(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "RestorePersistentBuffer future cb for "
                    "persistentBufferIndex #"
                        << persistentBufferIndex);
                const auto& result = f.GetValue();
                HandleListPersistentBufferResultOnRestore(
                    requestId, result, persistentBufferIndex, requestsStalking);
            });
    }
}

void TDirectBlockGroup::HandleListPersistentBufferResultOnRestore(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& result,
    size_t persistentBufferIndex,
    std::shared_ptr<TRequestsStalking> requestsStalking)
{
    LOG_DEBUG_S(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "HandleListPersistentBufferResultOnRestore for "
        "persistentBufferIndex #" << persistentBufferIndex);
    Y_UNUSED(storageRequestId);
    ++requestsStalking->ResponsesHandled;

    Y_ABORT_UNLESS(result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

    // Despite the fact, that this method can be invoked from different threads, we don't need locks.
    // The reason is each thread accesses strictly own its data and doesn't modify common memory.

    const auto& records = result.GetRecords();
    for (const auto& record: records) {
        const NKikimrBlobStorage::NDDisk::TBlockSelector& selector = record.GetSelector();
        const size_t startIndex = selector.GetOffsetInBytes() / BlockSize;
        const size_t blocksNumber = selector.GetSize() / BlockSize;

        for (size_t i = startIndex; i < startIndex + blocksNumber; ++i) {
            DirtyMap->SetLsnByPersistentBufferIndex(i, persistentBufferIndex,
                                                    record.GetLsn());
        }
    }

    if (requestsStalking->ResponsesHandled == requestsStalking->ResponsesExpected) {
        RestorePersistentBufferFinised(); // finish actor bootstrap
    }
}

void TDirectBlockGroup::RestorePersistentBufferFinised()
{
    LOG_DEBUG_S(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "RestorePersistentBufferFinised");
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

        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "WriteBlocksLocal" << " requestId# " << StorageRequestId);

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
            // TODO fix this. blockMeta is different for each meta in writesMeta
            const auto& writesMeta = requestHandler.GetWritesMeta();
            for (const auto& meta : writesMeta) {
                DirtyMap->OnBlockWriteCompleted(requestHandler.GetStartIndex(), meta);
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
    for (size_t i = 0; i < 3; i++) {
        auto syncRequestHandler = std::make_shared<TSyncRequestHandler>(
            requestHandler.GetStartIndex(),
            i, // persistentBufferIndex
            DirtyMap->GetLsnByPersistentBufferIndex(requestHandler.GetStartIndex(), i),
            requestHandler.Span.GetTraceId(),
            TabletId
        );

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

        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "ProcessSyncQueue" << " requestId# " << StorageRequestId);

        auto future = StorageTransport->Sync(
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
            HandleSyncResult(requestId, result);
        });

        RequestHandlersByStorageRequestId[StorageRequestId] = syncRequestHandler;
        SyncQueue.pop();
    }
}

void TDirectBlockGroup::HandleSyncResult(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvSyncResult& result)
{
    auto guard = Guard(Lock);

    if (!RequestHandlersByStorageRequestId.contains(storageRequestId)) {
        return;
    }

    auto& requestHandler = static_cast<TSyncRequestHandler&>(*RequestHandlersByStorageRequestId[storageRequestId]);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        DirtyMap->OnBlockFlushCompleted(requestHandler.GetStartIndex(), requestHandler.GetPersistentBufferIndex(),
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

    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "RequestBlockErase" << " requestId# " << StorageRequestId);

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
    if (!DirtyMap->IsBlockWritten(startIndex))
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

    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION, "ReadBlocksLocal" << " requestId# " << StorageRequestId);

    if (!DirtyMap->IsBlockFlushedToDDisk(startIndex)) {
        const auto& ddiskConnection = PersistentBufferConnections[0];

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
