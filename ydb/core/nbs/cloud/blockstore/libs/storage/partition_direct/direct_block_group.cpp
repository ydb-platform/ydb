#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
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
    TExecutorPtr executor,
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddisksIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds)
    : ActorSystem(actorSystem)
    , Executor(std::move(executor))
    , TabletId(tabletId)
    , StorageTransport(
          std::make_unique<NTransport::TICStorageTransport>(actorSystem))
{
    Y_ASSERT(persistentBufferDDiskIds.size() == DirectBlockGroupHostCount);
    Y_ASSERT(ddisksIds.size() == DirectBlockGroupHostCount);

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
        PBufferConnections,
        true);
}

TExecutorPtr TDirectBlockGroup::GetExecutor()
{
    return Executor;
}

ui64 TDirectBlockGroup::GenerateLsn()
{
    return ++LsnGenerator;
}

NThreading::TFuture<void> TDirectBlockGroup::EstablishConnections()
{
    for (size_t i = 0; i < DDiskConnections.size(); ++i) {
        DoEstablishConnection(EConnectionType::DDisk, i, DDiskConnections[i]);
    }

    for (size_t i = 0; i < PBufferConnections.size(); ++i) {
        DoEstablishConnection(
            EConnectionType::PBuffer,
            i,
            PBufferConnections[i]);
    }

    return ConnectionEstablishedPromise.GetFuture();
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroup::ReadBlocksFromDDisk(
    ui32 vChunkIndex,
    ui8 hostIndex,
    TBlockRange64 range,
    TGuardedSgList guardedSglist,
    NWilson::TTraceId traceId)
{
    // Executor thread.

    using TEvReadResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult>;

    if (!Initialized) {
        return MakeFuture<TDBGReadBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    const auto& ddiskConnection = PBufferConnections[hostIndex];

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.ReadBlocks.ReadDDisk",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->Read(
        ddiskConnection.GetServiceId(),
        ddiskConnection.Credentials,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        NKikimr::NDDisk::TReadInstruction(true),
        std::move(guardedSglist),
        childSpan);
    future.Subscribe(
        [promise = std::move(promise)]   //
        (const TEvReadResultFuture& f) mutable
        {
            const auto& result = f.GetValue();
            if (result.GetStatus() ==
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
                promise.SetValue(
                    TDBGReadBlocksResponse{.Error = MakeError(S_OK)});
            } else {
                promise.SetValue(TDBGReadBlocksResponse{
                    .Error = MakeError(E_FAIL, result.GetErrorReason())});
            }
        });
    return result;
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroup::ReadBlocksFromPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    TGuardedSgList guardedSglist,
    NWilson::TTraceId traceId)
{
    using TEvReadPersistentBufferResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>;

    if (!Initialized) {
        return MakeFuture<TDBGReadBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    const auto& ddiskConnection = PBufferConnections[hostIndex];

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.ReadBlocks.ReadPBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->ReadPersistentBuffer(
        ddiskConnection.GetServiceId(),
        ddiskConnection.Credentials,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TReadInstruction(true),
        std::move(guardedSglist),
        childSpan);
    future.Subscribe(
        [promise = std::move(promise)]   //
        (const TEvReadPersistentBufferResultFuture& f) mutable
        {
            const auto& result = f.GetValue();
            if (result.GetStatus() ==
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
                promise.SetValue(
                    TDBGReadBlocksResponse{.Error = MakeError(S_OK)});
            } else {
                promise.SetValue(TDBGReadBlocksResponse{
                    .Error = MakeError(E_FAIL, result.GetErrorReason())});
            }
        });
    return result;
}

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroup::WriteBlocksToPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    TGuardedSgList guardedSglist,
    NWilson::TTraceId traceId)
{
    using TEvWritePersistentBufferResultFuture = NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>;

    if (!Initialized) {
        return MakeFuture<TDBGWriteBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    const auto& connection = PBufferConnections[hostIndex];

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.WriteBlocks.WritePBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGWriteBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->WritePersistentBuffer(
        connection.GetServiceId(),
        connection.Credentials,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TWriteInstruction(0),
        std::move(guardedSglist),
        childSpan);
    future.Subscribe(
        [promise = std::move(promise)]   //
        (const TEvWritePersistentBufferResultFuture& f) mutable
        {
            const auto& result = f.GetValue();
            if (result.GetStatus() ==
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
                promise.SetValue(
                    TDBGWriteBlocksResponse{.Error = MakeError(S_OK)});
            } else {
                promise.SetValue(TDBGWriteBlocksResponse{
                    .Error = MakeError(E_FAIL, result.GetErrorReason())});
            }
        });
    return result;
}

NThreading::TFuture<TDBGSyncBlocksResponse>
TDirectBlockGroup::SyncWithPersistentBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    const TVector<TPBufferSegment>& segments,
    NWilson::TTraceId traceId)
{
    using TEvSyncWithPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult;

    const auto& ddiskConnection = DDiskConnections[hostIndex];
    const auto& persistentBufferConnection = PBufferConnections[hostIndex];

    TVector<NKikimr::NDDisk::TBlockSelector> selectors;
    TVector<ui64> lsns;
    for (const auto& segment: segments) {
        selectors.push_back(NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            segment.Range.Start * DefaultBlockSize,
            segment.Range.Size() * DefaultBlockSize));
        lsns.push_back(segment.Lsn);
    }

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.Flush.SyncWithPersistentBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto future = StorageTransport->SyncWithPersistentBuffer(
        ddiskConnection.GetServiceId(),
        ddiskConnection.Credentials,
        std::move(selectors),
        std::move(lsns),
        std::make_tuple(
            persistentBufferConnection.DDiskId.NodeId,
            persistentBufferConnection.DDiskId.PDiskId,
            persistentBufferConnection.DDiskId.DDiskSlotId),
        persistentBufferConnection.Credentials.DDiskInstanceGuid.value(),
        childSpan);

    auto promise = NewPromise<TDBGSyncBlocksResponse>();
    auto result = promise.GetFuture();

    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         segmentCount = segments.size()]   //
        (const TFuture<TEvSyncWithPersistentBufferResult>& f) mutable
        {
            // ActorSystem thread

            const TEvSyncWithPersistentBufferResult& response = f.GetValue();
            TDBGSyncBlocksResponse result;
            for (size_t i = 0; i < segmentCount; ++i) {
                const auto& segmentResult = response.GetSegmentResults(i);
                const bool ok =
                    segmentResult.GetStatus() ==
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK ||
                    segmentResult.GetStatus() ==
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED;
                result.Errors.push_back(MakeError(
                    ok ? S_OK : E_FAIL,
                    ok ? "" : segmentResult.GetErrorReason()));
            }

            executor->ExecuteSimple(
                [promise = std::move(promise), result = std::move(result)]   //
                () mutable
                {
                    // Executor thread
                    promise.SetValue(std::move(result));
                });
        });

    return result;
}

NThreading::TFuture<TDBGEraseResponse> TDirectBlockGroup::EraseFromPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    const TVector<TPBufferSegment>& segments,
    NWilson::TTraceId traceId)
{
    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;

    const auto& persistentBufferConnection = PBufferConnections[hostIndex];

    TVector<NKikimr::NDDisk::TBlockSelector> selectors;
    TVector<ui64> lsns;
    for (const auto& segment: segments) {
        selectors.push_back(NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            segment.Range.Start * DefaultBlockSize,
            segment.Range.Size() * DefaultBlockSize));
        lsns.push_back(segment.Lsn);
    }

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.Erase.ErasePBuffers",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto future = StorageTransport->ErasePersistentBuffer(
        persistentBufferConnection.GetServiceId(),
        persistentBufferConnection.Credentials,
        std::move(selectors),
        std::move(lsns),
        childSpan);

    auto promise = NewPromise<TDBGEraseResponse>();
    auto result = promise.GetFuture();

    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         segmentCount = segments.size()]   //
        (const TFuture<TEvErasePersistentBufferResult>& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [promise = std::move(promise),
                 result = UnsafeExtractValue(f)]   //
                () mutable
                {
                    // Executor thread
                    NProto::TError error;
                    if (result.GetStatus() ==
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK)
                    {
                        error = MakeError(S_OK);
                    } else {
                        error = MakeError(E_FAIL, result.GetErrorReason());
                    }
                    promise.SetValue(
                        TDBGEraseResponse{.Error = std::move(error)});
                });
        });

    return result;
}

NThreading::TFuture<TDBGRestoreResponse>
TDirectBlockGroup::RestoreFromPersistentBuffers(ui32 vChunkIndex)
{
    // Executor thread
    auto promise = NewPromise<TDBGRestoreResponse>();
    auto result = promise.GetFuture();

    ConnectionEstablishedPromise.GetFuture().Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         vChunkIndex]   //
        (const TFuture<void>&) mutable
        {
            // Executor thread
            if (auto self = weakSelf.lock()) {
                self->DoRestore(std::move(promise), vChunkIndex);
            } else {
                promise.SetValue(
                    TDBGRestoreResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return result;
}

/*
void TDirectBlockGroup::ErasePersistentBuffer(
    std::shared_ptr<TSyncAndEraseRequestHandler> requestHandler)
{
    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;

    const ui64 storageRequestId = ++StorageRequestId;

    auto future = StorageTransport->ErasePersistentBuffer(
        PBufferConnections[requestHandler->GetPersistentBufferIndex()]
            .GetServiceId(),
        PBufferConnections[requestHandler->GetPersistentBufferIndex()]
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
*/

void TDirectBlockGroup::DoEstablishConnection(
    EConnectionType connectionType,
    size_t index,
    const TDDiskConnection& connection)
{
    using TEvConnectResult = NKikimrBlobStorage::NDDisk::TEvConnectResult;

    auto future = StorageTransport->Connect(
        connection.GetServiceId(),
        connection.Credentials);

    future.Subscribe(
        [weakSelf = weak_from_this(),
         executor = Executor,
         connectionType,
         index]   //
        (const TFuture<TEvConnectResult>& f) mutable
        {
            executor->ExecuteSimple(
                [response = UnsafeExtractValue(f),
                 weakSelf = std::move(weakSelf),
                 connectionType,
                 index]   //
                () mutable
                {
                    if (auto self = weakSelf.lock()) {
                        self->HandleConnectionEstablished(
                            connectionType,
                            index,
                            std::move(response));
                    }
                });
        });
}

void TDirectBlockGroup::HandleConnectionEstablished(
    EConnectionType connectionType,
    size_t index,
    NKikimrBlobStorage::NDDisk::TEvConnectResult result)
{
    TDDiskConnection& connection = connectionType == DDisk
                                       ? DDiskConnections[index]
                                       : PBufferConnections[index];

    if (result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
        connection.Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
    } else {
        Y_ABORT(
            "TDirectBlockGroup::HandlePersistentBufferConnected: connection "
            "failed - unhandled error");
    }

    const bool allDDiskConnected = AllOf(
        DDiskConnections,
        [](const TDDiskConnection& c)
        { return c.Credentials.DDiskInstanceGuid.has_value(); });
    const bool allPBufferConnected = AllOf(
        PBufferConnections,
        [](const TDDiskConnection& c)
        { return c.Credentials.DDiskInstanceGuid.has_value(); });

    if (allDDiskConnected && allPBufferConnected) {
        Initialized = true;
        ConnectionEstablishedPromise.SetValue();
    }
}

void TDirectBlockGroup::DoRestore(
    NThreading::TPromise<TDBGRestoreResponse> promise,
    ui32 vChunkIndex)
{
    Y_UNUSED(vChunkIndex);

    promise.SetValue(
        TDBGRestoreResponse{.Error = MakeError(E_NOT_IMPLEMENTED)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
