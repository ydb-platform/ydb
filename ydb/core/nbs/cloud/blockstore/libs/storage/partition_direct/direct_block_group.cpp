#include "direct_block_group.h"

#include "restore_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TListPBufferResponse MakeListPBufferResponse(
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& response)
{
    TListPBufferResponse result;
    result.Error =
        response.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK
            ? MakeError(S_OK)
            : MakeError(E_FAIL, response.GetErrorReason());

    result.Meta.reserve(response.GetRecords().size());
    for (const auto& segment: response.GetRecords()) {
        ui64 lsn = segment.GetLsn();
        ui32 vChunkIndex = segment.GetSelector().GetVChunkIndex();
        auto range = TBlockRange64::WithLength(
            segment.GetSelector().GetOffsetInBytes() / DefaultBlockSize,
            segment.GetSelector().GetSize() / DefaultBlockSize);
        result.Meta.push_back(
            {.VChunkIndex = vChunkIndex, .Lsn = lsn, .Range = range});
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDBGWriteBlocksToManyPBuffersResponse
TDBGWriteBlocksToManyPBuffersResponse::MakeOverallError(
    EWellKnownResultCodes code,
    TString reason)
{
    TDBGWriteBlocksToManyPBuffersResponse result;
    result.OverallError = MakeError(code, std::move(reason));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool TDDiskIdLess::operator()(const TDDiskId& lh, const TDDiskId& rh) const
{
    auto makeTuple = [](const TDDiskId& item)
    {
        return std::make_tuple(
            item.GetNodeId(),
            item.GetPDiskId(),
            item.GetDDiskSlotId());
    };
    return makeTuple(lh) < makeTuple(rh);
}

const TFuture<NProto::TError>&
TDirectBlockGroup::TDDiskConnection::GetFuture() const
{
    return ConnectFuture;
}

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    NActors::TActorSystem* actorSystem,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TExecutorPtr executor,
    ui64 tabletId,
    ui32 generation,
    const TVector<NBsController::TDDiskId>& ddisksIds,
    const TVector<NBsController::TDDiskId>& pbufferIds)
    : ActorSystem(actorSystem)
    , Scheduler(std::move(scheduler))
    , Timer(std::move(timer))
    , Executor(std::move(executor))
    , TabletId(tabletId)
    , StorageTransport(
          std::make_unique<NTransport::TICStorageTransport>(actorSystem))
{
    Y_ASSERT(pbufferIds.size() == DirectBlockGroupHostCount);
    Y_ASSERT(ddisksIds.size() == DirectBlockGroupHostCount);

    auto addDDiskConnections = [&](const TVector<NBsController::TDDiskId>& ids,
                                   TVector<TDDiskConnection>& connections,
                                   EConnectionType type)
    {
        for (ui8 i = 0; i < ids.size(); ++i) {
            const auto& ddiskId = ids[i];
            connections.push_back(TDDiskConnection{
                .HostConnection = NTransport::THostConnection{
                    .ConnectionType = type,
                    .DDiskId = ddiskId,
                    .Credentials = NDDisk::TQueryCredentials(
                        TabletId,
                        generation,
                        std::nullopt,
                        type == EConnectionType::PBuffer)}});

            if (type == EConnectionType::PBuffer) {
                NKikimrBlobStorage::NDDisk::TDDiskId pbufferId;
                ddiskId.Serialize(&pbufferId);
                PBufferIdToHostIndex.insert({pbufferId, i});
            }
        }
    };

    addDDiskConnections(ddisksIds, DDiskConnections, EConnectionType::DDisk);
    addDDiskConnections(
        pbufferIds,
        PBufferConnections,
        EConnectionType::PBuffer);
}

TExecutorPtr TDirectBlockGroup::GetExecutor()
{
    return Executor;
}

void TDirectBlockGroup::Schedule(TDuration delay, TCallback callback)
{
    Scheduler->Schedule(
        Executor.get(),
        Timer->Now() + delay,
        std::move(callback));
}

void TDirectBlockGroup::EstablishConnections()
{
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this()]   //
        ()
        {
            if (auto self = weakSelf.lock()) {
                self->DoEstablishConnections();
            }
        });
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroup::ReadBlocksFromDDisk(
    ui32 vChunkIndex,
    ui8 hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
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

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.ReadBlocks.ReadDDisk",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->ReadFromDDisk(
        DDiskConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        NKikimr::NDDisk::TReadInstruction(true),
        guardedSglist,
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
    const TGuardedSgList& guardedSglist,
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

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.ReadBlocks.ReadPBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->ReadFromPBuffer(
        PBufferConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TReadInstruction(true),
        guardedSglist,
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
TDirectBlockGroup::WriteBlocksToDDisk(
    ui32 vChunkIndex,
    ui8 hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    NWilson::TTraceId traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvWriterResultFuture =
        NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvWriteResult>;

    if (!Initialized) {
        return MakeFuture<TDBGWriteBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.WriteBlocks.WriteDDisk",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGWriteBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->WriteToDDisk(
        DDiskConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        NKikimr::NDDisk::TWriteInstruction(0),
        guardedSglist,
        childSpan);
    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvWriterResultFuture& f) mutable
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

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroup::WriteBlocksToPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
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

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.WriteBlocks.WritePBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGWriteBlocksResponse>();
    auto result = promise.GetFuture();
    auto future = StorageTransport->WriteToPBuffer(
        PBufferConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TWriteInstruction(0),
        guardedSglist,
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

NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>
TDirectBlockGroup::WriteBlocksToManyPBuffers(
    ui32 vChunkIndex,
    std::vector<ui8> hostIndexes,
    ui64 lsn,
    TBlockRange64 range,
    TDuration replyTimeout,
    const TGuardedSgList& guardedSglist,
    NWilson::TTraceId traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(hostIndexes.size() > 0);

    using TEvWriteToManyPersistentBuffersResultFuture = NThreading::TFuture<
        NTransport::IStorageTransport::TEvWriteToManyPersistentBuffersResult>;

    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> disksIds(hostIndexes.size());
    for (auto hostIndex: hostIndexes) {
        const auto& ddiskId =
            PBufferConnections[hostIndex].HostConnection.DDiskId;

        disksIds.push_back({});
        ddiskId.Serialize(&disksIds.back());
    }

    if (!Initialized) {
        return MakeFuture<TDBGWriteBlocksToManyPBuffersResponse>(
            TDBGWriteBlocksToManyPBuffersResponse::MakeOverallError(
                E_REJECTED,
                "Connections are not established"));
    }

    auto childSpan = NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        "NbsPartition.WriteBlocks.WritePBuffers",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto promise = NewPromise<TDBGWriteBlocksToManyPBuffersResponse>();
    auto result = promise.GetFuture();

    auto future = StorageTransport->WriteToManyPBuffers(
        PBufferConnections[hostIndexes[0]].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TWriteInstruction(0),
        std::move(disksIds),
        replyTimeout,
        guardedSglist,
        childSpan);

    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         weakSelf = weak_from_this()](
            const TEvWriteToManyPersistentBuffersResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [promise = std::move(promise),
                 threadChecker,
                 f,
                 weakSelf = std::move(weakSelf)]() mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());
                    if (auto self = weakSelf.lock()) {
                        self->OnWriteBlocksToManyPBuffersResponse(
                            f.GetValue(),
                            std::move(promise));
                    } else {
                        promise.SetValue(
                            TDBGWriteBlocksToManyPBuffersResponse::
                                MakeOverallError(
                                    E_FAIL,
                                    "WriteBlocksToManyPBuffersResponse: DBG is "
                                    "destroyed already."));
                    }
                });
        });
    return result;
}

void TDirectBlockGroup::OnWriteBlocksToManyPBuffersResponse(
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffersResult& response,
    TPromise<TDBGWriteBlocksToManyPBuffersResponse> promise)
{
    TDBGWriteBlocksToManyPBuffersResponse dbgResponse;
    for (const auto& singlePBufferResponse: response.GetResult()) {
        auto hostIndex = PBufferIdToHostIndex.find(
            singlePBufferResponse.GetPersistentBufferId());
        if (hostIndex == PBufferIdToHostIndex.end()) {
            LOG_ERROR(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "TDBGWriteBlocksToManyPBuffersResponse: unexpected "
                "pbufferDiskId: %s",
                singlePBufferResponse.GetPersistentBufferId()
                    .ShortUtf8DebugString()
                    .c_str());
            continue;
        }
        Y_ABORT_UNLESS(
            PBufferConnections[hostIndex->second].HostConnection.DDiskId ==
            singlePBufferResponse.GetPersistentBufferId());

        NProto::TError error =
            singlePBufferResponse.GetResult().GetStatus() ==
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                ? MakeError(S_OK)
                : MakeError(
                      E_FAIL,
                      singlePBufferResponse.GetResult().GetErrorReason());

        dbgResponse.Responses.push_back(
            {.HostId = hostIndex->second, .Error = std::move(error)});
    }
    promise.SetValue(std::move(dbgResponse));
}

NThreading::TFuture<TDBGFlushResponse> TDirectBlockGroup::SyncWithPBuffer(
    ui32 vChunkIndex,
    ui8 pbufferHostIndex,
    ui8 ddiskHostIndex,
    const TVector<TPBufferSegment>& segments,
    NWilson::TTraceId traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvSyncWithPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvSyncWithPersistentBufferResult;

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
        "NbsPartition.Flush.SyncWithPBuffer",
        NWilson::EFlags::NONE,
        ActorSystem);

    auto future = StorageTransport->SyncWithPBuffer(
        PBufferConnections[pbufferHostIndex].HostConnection,
        DDiskConnections[ddiskHostIndex].HostConnection,
        std::move(selectors),
        std::move(lsns),
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

    auto future = StorageTransport->EraseFromPBuffer(
        PBufferConnections[hostIndex].HostConnection,
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

NThreading::TFuture<TDBGRestoreResponse> TDirectBlockGroup::RestoreDBGPBuffers(
    ui32 vChunkIndex)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto promise = NewPromise<TDBGRestoreResponse>();
    auto result = promise.GetFuture();

    RestoredPBuffersPromise.GetFuture().Subscribe(
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

NThreading::TFuture<TListPBufferResponse> TDirectBlockGroup::ListPBuffers(
    ui8 hostIndex)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (hostIndex >= PBufferConnections.size()) {
        return MakeFuture(TListPBufferResponse{.Error = MakeError(E_FAIL)});
    }

    const auto& connection = PBufferConnections[hostIndex];
    // Switch co-routine context if needed.
    const NProto::TError& connectError =
        Executor->WaitFor(connection.GetFuture());
    if (HasError(connectError)) {
        return MakeFuture(TListPBufferResponse{.Error = connectError});
    }

    auto promise = NewPromise<TListPBufferResponse>();
    auto result = promise.GetFuture();

    using TEvListPersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult;

    auto future =
        StorageTransport->ListPBufferEntries(connection.HostConnection);

    future.Subscribe(
        [promise = std::move(promise),
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TFuture<TEvListPersistentBufferResult>& f) mutable
        {
            // ActorSystem thread
            executor->ExecuteSimple(
                [promise = std::move(promise),
                 threadChecker,
                 f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    promise.SetValue(MakeListPBufferResponse(f.GetValue()));
                });
        });

    return result;
}

void TDirectBlockGroup::DoEstablishConnections()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (size_t i = 0; i < DDiskConnections.size(); ++i) {
        DoEstablishConnection(i, DDiskConnections[i]);
    }

    for (size_t i = 0; i < PBufferConnections.size(); ++i) {
        DoEstablishConnection(i, PBufferConnections[i]);
    }

    DoListPBuffers();
}

void TDirectBlockGroup::DoEstablishConnection(
    size_t index,
    const TDDiskConnection& connection)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvConnectResult = NKikimrBlobStorage::NDDisk::TEvConnectResult;

    auto future = StorageTransport->Connect(connection.HostConnection);

    future.Subscribe(
        [weakSelf = weak_from_this(),
         executor = Executor,
         connectionType = connection.HostConnection.ConnectionType,
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

    NProto::TError error =
        result.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK
            ? MakeError(S_OK)
            : MakeError(E_FAIL, result.GetErrorReason());
    if (!HasError(error)) {
        connection.HostConnection.Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
    } else {
        Y_ABORT(
            "TDirectBlockGroup::HandlePersistentBufferConnected: connection "
            "failed - unhandled error");
    }
    connection.ConnectPromise.SetValue(error);

    const bool allDDiskConnected = AllOf(
        DDiskConnections,
        [](const TDDiskConnection& c)
        { return c.HostConnection.IsConnected(); });
    const bool allPBufferConnected = AllOf(
        PBufferConnections,
        [](const TDDiskConnection& c)
        { return c.HostConnection.IsConnected(); });

    if (allDDiskConnected && allPBufferConnected) {
        Initialized = true;
        ConnectionEstablishedPromise.SetValue();
    }
}

void TDirectBlockGroup::DoListPBuffers()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    auto restoreExecutor = std::make_shared<TRestoreRequestExecutor>(
        ActorSystem,
        shared_from_this());

    auto future = restoreExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this()]   //
        (const NThreading::TFuture<TAggregatedListPBufferResponse>& f) mutable
        {
            // Executor thread
            if (auto self = weakSelf.lock()) {
                self->OnPBuffersListed(f.GetValue());
            }
        });

    restoreExecutor->Run();
}

void TDirectBlockGroup::OnPBuffersListed(
    const TAggregatedListPBufferResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (const auto& [hostIndex, metaVector]: response.Meta) {
        for (const auto& meta: metaVector) {
            auto& restoredPBuffer = RestoredPBuffers[meta.VChunkIndex];
            if (HasError(response.Error)) {
                restoredPBuffer.Error = response.Error;
            }
            restoredPBuffer.Meta.push_back(
                {.Lsn = meta.Lsn, .Range = meta.Range, .HostIndex = hostIndex});
        }
    }
    RestoredPBuffersPromise.SetValue();
}

void TDirectBlockGroup::DoRestore(
    NThreading::TPromise<TDBGRestoreResponse> promise,
    ui32 vChunkIndex)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    promise.SetValue(std::move(RestoredPBuffers[vChunkIndex]));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
