#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

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
    void TryUpdateLsnByPersistentBufferIndex(ui64 blockIndex, ui64 persistBufferIndex,
                                       ui64 lsn);
    [[nodiscard]] bool IsBlockWritten(ui64 blockIndex) const;
    [[nodiscard]] bool IsBlockFlushedToDDisk(ui64 blockIndex) const;
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

void TDirectBlockGroup::TDirtyMap::TryUpdateLsnByPersistentBufferIndex(
    ui64 blockIndex, ui64 persistBufferIndex, ui64 lsn)
{
    auto &curLsn = BlocksMeta[blockIndex].LsnByPersistentBufferIndex[persistBufferIndex];
    curLsn = std::max(curLsn, lsn);
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
    Executor = TExecutor::Create("DirectBlockGroup");
    Executor->Start();

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

TDirectBlockGroup::~TDirectBlockGroup()
{
    if (Executor) {
        Executor->Stop();
    }
}


void TDirectBlockGroup::EstablishConnections(NWilson::TTraceId traceId)
{
    auto requestHandler = std::make_shared<TOverallAckRequestHandler>(
        ActorSystem,
        std::move(traceId),
        "NbsPartition.EstablishConnections",
        TabletId,
        PersistentBufferConnections.size());
    for (size_t i = 0; i < PersistentBufferConnections.size(); i++) {
        Executor->ExecuteSimple(
            [weakSelf = weak_from_this(), i, requestHandler = requestHandler]()
            {
                if (auto self = weakSelf.lock()) {
                    self->DoEstablishPersistentBufferConnection(i, requestHandler);
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

void TDirectBlockGroup::DoEstablishPersistentBufferConnection(
    size_t i, std::shared_ptr<TOverallAckRequestHandler> requestHandler)
{
    auto future =
        StorageTransport->Connect(PersistentBufferConnections[i].GetServiceId(),
                                  PersistentBufferConnections[i].Credentials);

    const auto& resultOrError = Executor->ResultOrError(std::move(future));
    if (!HasError(resultOrError)) {
        HandlePersistentBufferConnected(i, resultOrError.GetResult(), requestHandler);
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
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result,
    std::shared_ptr<TOverallAckRequestHandler> requestHandler)
{
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        PersistentBufferConnections[index].Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();

        requestHandler->RegisterCompetedRequest();
    } else {
        Y_ABORT("TDirectBlockGroup::HandlePersistentBufferConnected: connection failed - unhandled error");
    }

    if (requestHandler->IsCompleted()) {
        LOG_DEBUG_S(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TDirectBlockGroup::HandlePersistentBufferConnected finished");
        RestoreFromPersistentBuffer(requestHandler->Span.GetTraceId());
    }
}

void TDirectBlockGroup::RestoreFromPersistentBuffer(NWilson::TTraceId traceId)
{
    auto requestHandler = std::make_shared<TOverallAckRequestHandler>(
        ActorSystem,
        std::move(traceId),
        "NbsPartition.RestoreFromPersistentBuffer",
        TabletId,
        PersistentBufferConnections.size()
    );

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(), requestHandler]()
        {
            if (auto self = weakSelf.lock()) {
                self->DoRestoreFromPersistentBuffer(requestHandler);
            }
        });
}

// TODO заблокировать IO до полного восстановления (где-то раньше)
void TDirectBlockGroup::DoRestoreFromPersistentBuffer(
    std::shared_ptr<TOverallAckRequestHandler> requestHandler
)
{
    using TEvListPersistentBufferResult =
        TFuture<NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult>;

    TVector<TEvListPersistentBufferResult> futures;
    TVector<ui64> storageRequestIds;

    for (size_t i = 0; i < requestHandler->GetRequiredAckCount(); i++) {
        const auto& ddiskConnection = PersistentBufferConnections[i];
        ++StorageRequestId;
        storageRequestIds.push_back(StorageRequestId);

        auto future = StorageTransport->ListPersistentBuffer(
            ddiskConnection.GetServiceId(), ddiskConnection.Credentials,
            StorageRequestId);

        futures.push_back(std::move(future));
    }

    for (size_t i = 0; i < requestHandler->GetRequiredAckCount(); i++) {
        const auto& resultOrError =
            Executor->ResultOrError(std::move(futures[i]));

        if (!HasError(resultOrError)) {
            HandleListPersistentBufferResultOnRestore(
                storageRequestIds[i],
                resultOrError.GetResult(),
                i,
                requestHandler);
        } else {
            Y_ABORT("TDirectBlockGroup::DoRestoreFromPersistentBuffer: connection failed - unhandled error");
        }
    }
}

void TDirectBlockGroup::HandleListPersistentBufferResultOnRestore(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& result,
    size_t persistentBufferIndex,
    std::shared_ptr<TOverallAckRequestHandler> requestHandler)
{
    LOG_DEBUG_S(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TDirectBlockGroup::HandleListPersistentBufferResultOnRestore " << persistentBufferIndex
    );

    Y_UNUSED(storageRequestId);
    requestHandler->RegisterCompetedRequest();

    Y_ABORT_UNLESS(result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

    // Despite the fact, that this method can be invoked from different threads, we don't need locks.
    // The reason is each thread accesses strictly own its data and doesn't modify common memory.

    const auto& records = result.GetRecords();
    for (const auto& record: records) {
        const NKikimrBlobStorage::NDDisk::TBlockSelector& selector = record.GetSelector();
        const size_t startIndex = selector.GetOffsetInBytes() / BlockSize;
        const size_t blocksNumber = selector.GetSize() / BlockSize;

        for (size_t i = startIndex; i < startIndex + blocksNumber; ++i) {
            DirtyMap->TryUpdateLsnByPersistentBufferIndex(
                i, persistentBufferIndex, record.GetLsn());
        }
    }

    if (requestHandler->IsCompleted()) {
        RestoreFromPersistentBufferFinised(); // finish actor bootstrap
    }
}

void TDirectBlockGroup::RestoreFromPersistentBufferFinised()
{
    // function's body will be writen a bit later
    LOG_DEBUG_S(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TDirectBlockGroup::RestoreFromPersistentBufferFinised"
    );
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
        return;
    }

    const ui64 storageRequestId = ++StorageRequestId;

    if (!DirtyMap->IsBlockFlushedToDDisk(startIndex)) {
        const auto& ddiskConnection = PersistentBufferConnections[0];

        auto& childSpan = requestHandler->GetChildSpan(storageRequestId, true);
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
