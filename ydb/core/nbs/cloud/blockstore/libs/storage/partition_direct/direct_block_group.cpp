#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

using TEvReadPersistentBufferResult =
    NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult;
using TEvReadResult = NKikimrBlobStorage::NDDisk::TEvReadResult;

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    NActors::TActorSystem* actorSystem,
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddisksIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds)
    : ActorSystem(actorSystem)
    , TabletId(tabletId)
    , StorageTransport(
          std::make_unique<NTransport::TICStorageTransport>(actorSystem))
{
    Y_ASSERT(
        persistentBufferDDiskIds.size() == TDirectBlockGroup::DDisksNumber);
    Y_ASSERT(ddisksIds.size() == TDirectBlockGroup::DDisksNumber);

    auto addDDiskConnections = [&](TVector<NBsController::TDDiskId> ddisksIds,
                                   TVector<TDDiskConnection>& ddiskConnections,
                                   bool fromPersistentBuffer)
    {
        for (const auto& ddiskId: ddisksIds) {
            ddiskConnections.emplace_back(
                ddiskId,
                NDDisk::TQueryCredentials(
                    TabletId,
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

NThreading::TFuture<void> TDirectBlockGroup::EstablishConnections(
    TExecutorPtr executor,
    NWilson::TTraceId traceId,
    ui32 vChunkIndex)
{
    auto requestHandler = std::make_shared<TOverallAckRequestHandler>(
        ActorSystem,
        std::move(traceId),
        "NbsPartition.EstablishConnections",
        TabletId,
        vChunkIndex,
        PersistentBufferConnections.size());

    for (size_t i = 0; i < DDiskConnections.size(); i++) {
        DoEstablishDDiskConnection(executor, i);
    }

    for (size_t i = 0; i < PersistentBufferConnections.size(); i++) {
        DoEstablishPersistentBufferConnection(executor, i, requestHandler);
    }

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::DoEstablishPersistentBufferConnection(
    TExecutorPtr executor,
    size_t i,
    std::shared_ptr<TOverallAckRequestHandler> requestHandler)
{
    LOG_DEBUG_S(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "DoEstablishPersistentBufferConnection: " << i);
    auto future = StorageTransport->Connect(
        PersistentBufferConnections[i].GetServiceId(),
        PersistentBufferConnections[i].Credentials);

    const auto& resultOrError = executor->ResultOrError(std::move(future));
    if (!HasError(resultOrError)) {
        HandlePersistentBufferConnected(
            i,
            resultOrError.GetResult(),
            requestHandler);
    }
    // TODO: add error handling
}

void TDirectBlockGroup::DoEstablishDDiskConnection(
    TExecutorPtr executor,
    size_t i)
{
    auto future = StorageTransport->Connect(
        DDiskConnections[i].GetServiceId(),
        DDiskConnections[i].Credentials);

    const auto& resultOrError = executor->ResultOrError(std::move(future));
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
    LOG_DEBUG_S(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "HandlePersistentBufferConnected: " << index);
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        PersistentBufferConnections[index].Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();

        requestHandler->RegisterCompetedRequest();
    } else {
        Y_ABORT(
            "TDirectBlockGroup::HandlePersistentBufferConnected: connection "
            "failed - unhandled error");
    }

    if (requestHandler->IsCompleted()) {
        LOG_INFO_S(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "Connecting to persistent buffers has been finished");

        requestHandler->SetResponse();
    }
}

TVector<TRestoreMeta> TDirectBlockGroup::RestoreFromPersistentBuffers(
    TExecutorPtr executor,
    NWilson::TTraceId traceId,
    ui32 vChunkIndex)
{
    LOG_INFO_S(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "Restoring from persistent buffer started");
    auto requestHandler = std::make_shared<TOverallAckRequestHandler>(
        ActorSystem,
        std::move(traceId),
        "NbsPartition.RestoreFromPersistentBuffer",
        TabletId,
        vChunkIndex,
        PersistentBufferConnections.size());

    return DoRestoreFromPersistentBuffers(executor, std::move(requestHandler));
}

TVector<TRestoreMeta> TDirectBlockGroup::DoRestoreFromPersistentBuffers(
    TExecutorPtr executor,
    std::shared_ptr<TOverallAckRequestHandler> requestHandler)
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
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            StorageRequestId);

        futures.push_back(std::move(future));
    }

    TVector<TRestoreMeta> restoreLsnMeta;
    for (size_t i = 0; i < requestHandler->GetRequiredAckCount(); i++) {
        const auto& resultOrError =
            executor->ResultOrError(std::move(futures[i]));

        if (!HasError(resultOrError)) {
            HandleListPersistentBufferResultOnRestore(
                storageRequestIds[i],
                resultOrError.GetResult(),
                i,
                requestHandler,
                &restoreLsnMeta);
        } else {
            Y_ABORT(
                "TDirectBlockGroup::DoRestoreFromPersistentBuffer: connection "
                "failed - unhandled error");
        }
    }

    return restoreLsnMeta;
}

void TDirectBlockGroup::HandleListPersistentBufferResultOnRestore(
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& result,
    size_t persistentBufferIndex,
    std::shared_ptr<TOverallAckRequestHandler> requestHandler,
    TVector<TRestoreMeta>* restoreLsnMeta)
{
    LOG_DEBUG_S(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TDirectBlockGroup::HandleListPersistentBufferResultOnRestore "
            << persistentBufferIndex);

    Y_UNUSED(storageRequestId);
    requestHandler->RegisterCompetedRequest();

    Y_ABORT_UNLESS(
        result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

    // Despite the fact, that this method can be invoked from different threads,
    // we don't need locks. The reason is each thread accesses strictly own its
    // data and doesn't modify common memory.

    const auto& records = result.GetRecords();
    for (const auto& record: records) {
        const NKikimrBlobStorage::NDDisk::TBlockSelector& selector =
            record.GetSelector();
        const size_t startIndex = selector.GetOffsetInBytes() / 4096;
        const size_t blocksNumber = selector.GetSize() / 4096;

        for (size_t i = startIndex; i < startIndex + blocksNumber; ++i) {
            restoreLsnMeta->emplace_back(
                i,
                static_cast<ui64>(persistentBufferIndex),
                record.GetLsn());
        }
    }

    if (requestHandler->IsCompleted()) {
        RestoreFromPersistentBufferFinised(
            requestHandler->Span.GetTraceId(),
            requestHandler->GetVChunkIndex());
    }
}

void TDirectBlockGroup::RestoreFromPersistentBufferFinised(
    NWilson::TTraceId traceId,
    ui32 vChunkIndex)
{
    Y_UNUSED(traceId);
    Y_UNUSED(vChunkIndex);
    LOG_INFO_S(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "Restoring from persistent buffer finished");

    Initialized = true;

    // TODO uncomment it after unittests
    /*
    LOG_INFO_S(*ActorSystem, NKikimrServices::NBS_PARTITION,
                "Starting to flush dirtyMap");
    DirtyMap->VisitEachBlockMeta([this, &traceId, vChunkIndex](ui64 blockIndex,
    const TBlockMeta& blockMeta) { if (blockMeta.ReadyToFlush()) {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::NBS_PARTITION,
                "Trying to flush block " << blockIndex);

            RequestBlockFlush(NWilson::TTraceId(traceId), blockIndex,
    vChunkIndex);
        }
    });

    */
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

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroup::WriteBlocksLocal(
    ui32 vChunkIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TWriteRequestHandler>(
        ActorSystem,
        vChunkIndex,
        std::move(request),
        std::move(traceId),
        TabletId);

    if (!Initialized) {
        requestHandler->SetResponse(
            MakeError(E_REJECTED, "Connections are not established"));
    } else {
        DoWriteBlocksLocal(requestHandler);
    }

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::DoWriteBlocksLocal(
    std::shared_ptr<TWriteRequestHandler> requestHandler)
{
    using TEvWritePersistentBufferResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>;

    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        requestHandler->Span.GetTraceId(),
        "NbsPartition.WriteBlocks.PBWrite.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    for (size_t i = 0; i < 3; i++) {
        execSpan.Event("PB request start");
        const ui64 storageRequestId = ++StorageRequestId;
        const auto& ddiskConnection = PersistentBufferConnections[i];

        auto& childSpan = requestHandler->GetChildSpan(storageRequestId, i);
        auto future = StorageTransport->WritePersistentBuffer(
            ddiskConnection.GetServiceId(),
            ddiskConnection.Credentials,
            NKikimr::NDDisk::TBlockSelector(
                requestHandler->GetVChunkIndex(),
                requestHandler->GetStartOffset(),
                requestHandler->GetSize()),
            storageRequestId,   // lsn
            NKikimr::NDDisk::TWriteInstruction(0),
            requestHandler->GetData(),
            childSpan);

        requestHandler->OnWriteRequested(storageRequestId, i, storageRequestId);
        future.Subscribe(
            [requestHandler,
             storageRequestId](const TEvWritePersistentBufferResultFuture& f) {
                requestHandler->OnWriteFinished(storageRequestId, f.GetValue());
            });

        execSpan.Event("PB request end");
    }

    execSpan.EndOk();
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TDBGSyncBlocksResponse>
TDirectBlockGroup::SyncWithPersistentBuffer(
    ui32 vChunkIndex,
    ui8 persistBufferIndex,
    const TVector<TSyncRequest>& syncRequests,
    NWilson::TTraceId traceId)
{
    using TEvSyncWithPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult;

    auto requestHandler = std::make_shared<TSyncAndEraseRequestHandler>(
        ActorSystem,
        vChunkIndex,
        persistBufferIndex,
        std::move(traceId),
        TabletId,
        syncRequests);

    const auto& ddiskConnection = DDiskConnections[persistBufferIndex];
    const auto& persistentBufferConnection =
        PersistentBufferConnections[persistBufferIndex];

    const ui64 storageRequestId = ++StorageRequestId;

    auto future = StorageTransport->SyncWithPersistentBuffer(
        ddiskConnection.GetServiceId(),
        ddiskConnection.Credentials,
        requestHandler->GetBlockSelectors(),
        requestHandler->GetLsns(),
        std::make_tuple(
            persistentBufferConnection.DDiskId.NodeId,
            persistentBufferConnection.DDiskId.PDiskId,
            persistentBufferConnection.DDiskId.DDiskSlotId),
        persistentBufferConnection.Credentials.DDiskInstanceGuid.value(),
        requestHandler->GetChildSpan(storageRequestId));

    future.Subscribe(
        [weakSelf = weak_from_this(), requestHandler, storageRequestId]   //
        (const TFuture<TEvSyncWithPersistentBufferResult>& f) mutable
        {
            if (auto self = weakSelf.lock()) {
                self->HandleSyncWithPersistentBufferResult(
                    std::move(requestHandler),
                    storageRequestId,
                    UnsafeExtractValue(f));
            } else {
                requestHandler->SetResponse(MakeError(E_CANCELLED));
            }
        });
    return requestHandler->GetFuture();
}

void TDirectBlockGroup::HandleSyncWithPersistentBufferResult(
    std::shared_ptr<TSyncAndEraseRequestHandler> requestHandler,
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult& result)
{
    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK ||
        result.GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED)
    {
        requestHandler->ChildSpanEndOk(storageRequestId);

        ErasePersistentBuffer(std::move(requestHandler));
    } else {
        // TODO: add error handling
        requestHandler->ChildSpanEndError(
            storageRequestId,
            "HandleSyncWithPersistentBufferResult failed");
        requestHandler->Span.EndError(
            "HandleSyncWithPersistentBufferResult failed");

        requestHandler->SetResponse(MakeError(result.GetStatus()));
    }
}

void TDirectBlockGroup::ErasePersistentBuffer(
    std::shared_ptr<TSyncAndEraseRequestHandler> requestHandler)
{
    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;

    const ui64 storageRequestId = ++StorageRequestId;

    auto future = StorageTransport->ErasePersistentBuffer(
        PersistentBufferConnections[requestHandler->GetPersistentBufferIndex()]
            .GetServiceId(),
        PersistentBufferConnections[requestHandler->GetPersistentBufferIndex()]
            .Credentials,
        requestHandler->GetBlockSelectors(),
        requestHandler->GetLsns(),
        requestHandler->GetChildSpan(storageRequestId));

    future.Subscribe(
        [weakSelf = weak_from_this(), requestHandler, storageRequestId]   //
        (const TFuture<TEvErasePersistentBufferResult>& f) mutable
        {
            if (auto self = weakSelf.lock()) {
                self->HandleErasePersistentBufferResult(
                    std::move(requestHandler),
                    storageRequestId,
                    UnsafeExtractValue(f));
            } else {
                requestHandler->ChildSpanEndError(
                    storageRequestId,
                    "HandleEraseResult canceled");
                requestHandler->Span.EndError("HandleEraseResult canceled");
                requestHandler->SetResponse(MakeError(E_CANCELLED));
            }
        });
}

void TDirectBlockGroup::HandleErasePersistentBufferResult(
    std::shared_ptr<TSyncAndEraseRequestHandler> requestHandler,
    ui64 storageRequestId,
    const NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult& result)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.WriteBlocks.HandleErasePersistentBufferResult.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->ChildSpanEndOk(storageRequestId);
        requestHandler->Span.EndOk();
        requestHandler->SetResponse(MakeError(S_OK));
    } else {
        // TODO: add error handling
        requestHandler->ChildSpanEndError(
            storageRequestId,
            "HandleEraseResult failed");
        requestHandler->Span.EndError("HandleEraseResult failed");
        requestHandler->SetResponse(MakeError(result.GetStatus()));
    }

    execSpan.EndOk();
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroup::ReadBlocksLocalFromPersistentBuffer(
    ui32 vChunkIndex,
    ui8 persistentBufferIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 lsn)
{
    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TReadRequestHandler>(
        ActorSystem,
        vChunkIndex,
        std::move(request),
        std::move(traceId),
        TabletId);

    if (!Initialized) {
        requestHandler->SetResponse(
            MakeError(E_REJECTED, "Connections are not established"));
    } else {
        DoReadBlocksLocalFromPersistentBuffer(
            requestHandler,
            persistentBufferIndex,
            lsn);
    }

    return requestHandler->GetFuture();
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroup::ReadBlocksLocalFromDDisk(
    ui32 vChunkIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(callContext);

    auto requestHandler = std::make_shared<TReadRequestHandler>(
        ActorSystem,
        vChunkIndex,
        std::move(request),
        std::move(traceId),
        TabletId);
    if (!Initialized) {
        requestHandler->SetResponse(
            MakeError(E_REJECTED, "Connections are not established"));
    } else {
        DoReadBlocksLocalFromDDisk(requestHandler);
    }

    return requestHandler->GetFuture();
}

void TDirectBlockGroup::DoReadBlocksLocalFromPersistentBuffer(
    std::shared_ptr<TReadRequestHandler> requestHandler,
    ui8 persistentBufferIndex,
    ui64 lsn)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.ReadBlocks.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    const ui64 storageRequestId = ++StorageRequestId;
    const auto& ddiskConnection =
        PersistentBufferConnections[persistentBufferIndex];

    auto& childSpan =
        requestHandler->GetChildSpan(storageRequestId, persistentBufferIndex);

    auto future = StorageTransport->ReadPersistentBuffer(
        ddiskConnection.GetServiceId(),
        ddiskConnection.Credentials,
        NKikimr::NDDisk::TBlockSelector(
            requestHandler->GetVChunkIndex(),
            requestHandler->GetStartOffset(),
            requestHandler->GetSize()),
        lsn,
        NKikimr::NDDisk::TReadInstruction(true),
        requestHandler->GetData(),
        childSpan);

    execSpan.EndOk();

    future.Subscribe(
        [actorSystem = ActorSystem,
         requestHandler = std::move(requestHandler),
         storageRequestId]   //
        (const TFuture<TEvReadPersistentBufferResult>& f) mutable
        {
            HandleReadResult(
                actorSystem,
                std::move(requestHandler),
                storageRequestId,
                f.GetValue());
        });
}

void TDirectBlockGroup::DoReadBlocksLocalFromDDisk(
    std::shared_ptr<TReadRequestHandler> requestHandler)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.ReadBlocks.Exec",
        NWilson::EFlags::NONE,
        ActorSystem);

    const ui64 storageRequestId = ++StorageRequestId;
    const auto& ddiskConnection = DDiskConnections[0];

    auto& childSpan = requestHandler->GetChildSpan(storageRequestId, false);
    auto future = StorageTransport->Read(
        ddiskConnection.GetServiceId(),
        ddiskConnection.Credentials,
        NKikimr::NDDisk::TBlockSelector(
            requestHandler->GetVChunkIndex(),
            requestHandler->GetStartOffset(),
            requestHandler->GetSize()),
        NKikimr::NDDisk::TReadInstruction(true),
        requestHandler->GetData(),
        childSpan);

    execSpan.EndOk();

    future.Subscribe(
        [actorSystem = ActorSystem,
         requestHandler = std::move(requestHandler),
         storageRequestId]   //
        (const NThreading::TFuture<TEvReadResult>& f) mutable
        {
            HandleReadResult(
                actorSystem,
                std::move(requestHandler),
                storageRequestId,
                f.GetValue());
        });
}

// static
template <typename TEvent>
void TDirectBlockGroup::HandleReadResult(
    NActors::TActorSystem* actorSystem,
    std::shared_ptr<TReadRequestHandler> requestHandler,
    ui64 storageRequestId,
    const TEvent& result)
{
    auto execSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(requestHandler->Span.GetTraceId()),
        "NbsPartition.ReadBlocks.HandleReadResult.Exec",
        NWilson::EFlags::NONE,
        actorSystem);

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        requestHandler->ChildSpanEndOk(storageRequestId);
        requestHandler->SetResponse(MakeError(S_OK));
        requestHandler->Span.EndOk();
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
