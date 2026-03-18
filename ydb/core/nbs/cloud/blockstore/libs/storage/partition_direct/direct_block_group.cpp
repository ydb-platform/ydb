#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId TDirectBlockGroup::TDDiskConnection::GetServiceId() const
{
    return NKikimr::MakeBlobStorageDDiskId(
        DDiskId.NodeId,
        DDiskId.PDiskId,
        DDiskId.DDiskSlotId);
}

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    NActors::TActorSystem* actorSystem,
    TExecutorPtr executor,
    ui64 tabletId,
    ui32 generation,
    const TVector<NBsController::TDDiskId>& ddisksIds,
    const TVector<NBsController::TDDiskId>& pbufferIds)
    : ActorSystem(actorSystem)
    , Executor(std::move(executor))
    , TabletId(tabletId)
    , StorageTransport(
          std::make_unique<NTransport::TICStorageTransport>(actorSystem))
{
    Y_ASSERT(pbufferIds.size() == DirectBlockGroupHostCount);
    Y_ASSERT(ddisksIds.size() == DirectBlockGroupHostCount);

    auto addDDiskConnections = [&](const TVector<NBsController::TDDiskId>& ids,
                                   TVector<TDDiskConnection>& connections,
                                   bool fromPersistentBuffer)
    {
        for (const auto& ddiskId: ids) {
            connections.emplace_back(
                ddiskId,
                NDDisk::TQueryCredentials(
                    TabletId,
                    generation,
                    std::nullopt,
                    fromPersistentBuffer));
        }
    };

    addDDiskConnections(ddisksIds, DDiskConnections, false);
    addDDiskConnections(pbufferIds, PBufferConnections, true);
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
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this()]   //
        ()
        {
            if (auto self = weakSelf.lock()) {
                self->DoEstablishConnections();
            }
        });
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
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvReadResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult>;

    if (!Initialized) {
        return MakeFuture<TDBGReadBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    const auto& connection = DDiskConnections[hostIndex];

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.ReadBlocks.ReadDDisk",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->ReadFromDDisk(
        connection.GetServiceId(),
        connection.Credentials,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        NKikimr::NDDisk::TReadInstruction(true),
        std::move(guardedSglist),
        childSpan);
    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvReadResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [promise = std::move(promise), threadChecker, f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    const auto& response = f.GetValue();
                    NProto::TError error =
                        response.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, response.GetErrorReason());

                    promise.SetValue(
                        TDBGReadBlocksResponse{.Error = std::move(error)});
                });
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
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvReadPersistentBufferResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>;

    if (!Initialized) {
        return MakeFuture<TDBGReadBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    const auto& connection = PBufferConnections[hostIndex];

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.ReadBlocks.ReadPBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->ReadFromPBuffer(
        connection.GetServiceId(),
        connection.Credentials,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TReadInstruction(true),
        std::move(guardedSglist),
        childSpan);
    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvReadPersistentBufferResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [promise = std::move(promise), threadChecker, f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    const auto& response = f.GetValue();
                    NProto::TError error =
                        response.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, response.GetErrorReason());

                    promise.SetValue(
                        TDBGReadBlocksResponse{.Error = std::move(error)});
                });
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
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

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
        [promise = std::move(promise),
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvWritePersistentBufferResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [promise = std::move(promise), threadChecker, f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    const auto& response = f.GetValue();
                    NProto::TError error =
                        response.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, response.GetErrorReason());

                    promise.SetValue(
                        TDBGWriteBlocksResponse{.Error = std::move(error)});
                });
        });
    return result;
}

NThreading::TFuture<TDBGFlushResponse> TDirectBlockGroup::FlushFromPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    const TVector<TPBufferSegment>& segments,
    NWilson::TTraceId traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

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
        "NbsPartition.Flush.FlushFromPBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto future = StorageTransport->FlushFromPBuffer(
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

    auto promise = NewPromise<TDBGFlushResponse>();
    auto result = promise.GetFuture();

    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         segmentCount = segments.size()]   //
        (const TFuture<TEvSyncWithPersistentBufferResult>& f) mutable
        {
            // ActorSystem thread

            const TEvSyncWithPersistentBufferResult& response = f.GetValue();
            TDBGFlushResponse result;
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
                [promise = std::move(promise),
                 result = std::move(result),
                 threadChecker]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

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
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

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
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         segmentCount = segments.size()]   //
        (const TFuture<TEvErasePersistentBufferResult>& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [promise = std::move(promise),
                 threadChecker,
                 result = UnsafeExtractValue(f)]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    NProto::TError error =
                        result.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, result.GetErrorReason());
                    promise.SetValue(
                        TDBGEraseResponse{.Error = std::move(error)});
                });
        });

    return result;
}

NThreading::TFuture<TDBGRestoreResponse>
TDirectBlockGroup::RestoreFromPersistentBuffers(ui32 vChunkIndex)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto promise = NewPromise<TDBGRestoreResponse>();
    auto result = promise.GetFuture();

    ConnectionEstablishedPromise.GetFuture().Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         vChunkIndex]   //
        (const TFuture<void>&) mutable
        {
            Y_ABORT_UNLESS(threadChecker.Check());

            if (auto self = weakSelf.lock()) {
                self->DoRestore(std::move(promise), vChunkIndex);
            } else {
                promise.SetValue(
                    TDBGRestoreResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return result;
}

void TDirectBlockGroup::DoEstablishConnections()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (size_t i = 0; i < DDiskConnections.size(); ++i) {
        DoEstablishConnection(EConnectionType::DDisk, i, DDiskConnections[i]);
    }

    for (size_t i = 0; i < PBufferConnections.size(); ++i) {
        DoEstablishConnection(
            EConnectionType::PBuffer,
            i,
            PBufferConnections[i]);
    }
}

void TDirectBlockGroup::DoEstablishConnection(
    EConnectionType connectionType,
    size_t index,
    const TDDiskConnection& connection)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

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
                [weakSelf = std::move(weakSelf), connectionType, index, f]   //
                () mutable
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnConnectionEstablished(
                            connectionType,
                            index,
                            f.GetValue());
                    }
                });
        });
}

void TDirectBlockGroup::OnConnectionEstablished(
    EConnectionType connectionType,
    size_t index,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TDDiskConnection& connection = connectionType == EConnectionType::DDisk
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
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    Y_UNUSED(vChunkIndex);

    promise.SetValue(
        TDBGRestoreResponse{.Error = MakeError(E_NOT_IMPLEMENTED)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
