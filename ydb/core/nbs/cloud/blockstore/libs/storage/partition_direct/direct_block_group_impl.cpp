#include "direct_block_group_impl.h"

#include "restore_request.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto DefaultOracleThinkInterval = TDuration::Seconds(1);

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

const TFuture<NProto::TError>&
TDirectBlockGroup::TDDiskConnection::GetFuture() const
{
    return ConnectFuture;
}

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    NActors::TActorSystem* actorSystem,
    TStorageConfigPtr storageConfig,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TExecutorPtr executor,
    ui64 tabletId,
    ui32 generation,
    const TVector<NBsController::TDDiskId>& ddisksIds,
    const TVector<NBsController::TDDiskId>& pbufferIds)
    : ActorSystem(actorSystem)
    , StorageConfig(std::move(storageConfig))
    , Scheduler(std::move(scheduler))
    , Timer(std::move(timer))
    , Executor(std::move(executor))
    , TabletId(tabletId)
    , StorageTransport(
          std::make_unique<NTransport::TICStorageTransport>(actorSystem))
    , Oracle(StorageConfig, this, HostStatistics, HostStates)
{
    Y_ASSERT(pbufferIds.size() == DirectBlockGroupHostCount);
    Y_ASSERT(ddisksIds.size() == DirectBlockGroupHostCount);

    HostStatistics.resize(DirectBlockGroupHostCount);
    HostStates.resize(DirectBlockGroupHostCount);

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

    ScheduleOracleThinking();
}

void TDirectBlockGroup::Register(TVChunkWeakPtr vChunk)
{
    VChunks.push_back(std::move(vChunk));
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

std::shared_ptr<NWilson::TSpan> TDirectBlockGroup::CreateChildSpan(
    const NWilson::TTraceId& traceId,
    TStringBuf name)
{
    if (!traceId) {
        return nullptr;
    }
    return std::make_shared<NWilson::TSpan>(
        NKikimr::TWilsonNbs::NbsBasic,
        traceId.Clone(),
        TString(name),
        NWilson::EFlags::AUTO_END,
        ActorSystem);
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
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvReadResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult>;

    if (!Initialized) {
        return MakeFuture<TDBGReadBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.ReadBlocks.ReadDDisk");

    auto startAt = TMonotonic::Now();
    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    OnRequest(hostIndex, EOperation::ReadFromDDisk);
    auto future = StorageTransport->ReadFromDDisk(
        DDiskConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        NKikimr::NDDisk::TReadInstruction(true),
        guardedSglist,
        childSpan.get());
    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         childSpan = std::move(childSpan),
         hostIndex,
         startAt,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvReadResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [weakSelf,
                 promise = std::move(promise),
                 childSpan = std::move(childSpan),
                 hostIndex,
                 startAt,
                 threadChecker,
                 f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    const auto& response = f.GetValue();
                    NProto::TError error =
                        response.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, response.GetErrorReason());

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::ReadFromDDisk,
                            error);
                    }

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
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvReadPersistentBufferResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>;

    const auto startAt = TMonotonic::Now();

    if (!Initialized) {
        return MakeFuture<TDBGReadBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.ReadBlocksFromPBuffer");

    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    OnRequest(hostIndex, EOperation::ReadFromPBuffer);
    auto future = StorageTransport->ReadFromPBuffer(
        PBufferConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TReadInstruction(true),
        guardedSglist,
        childSpan.get());
    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         childSpan = std::move(childSpan),
         hostIndex,
         startAt,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvReadPersistentBufferResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [weakSelf,
                 promise = std::move(promise),
                 childSpan = std::move(childSpan),
                 hostIndex,
                 startAt,
                 threadChecker,
                 f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    const auto& response = f.GetValue();
                    NProto::TError error =
                        response.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, response.GetErrorReason());

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::ReadFromPBuffer,
                            error);
                    }

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
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvWriterResultFuture =
        NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvWriteResult>;

    const auto startAt = TMonotonic::Now();

    if (!Initialized) {
        return MakeFuture<TDBGWriteBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.WriteBlocksToDDisk");

    auto promise = NewPromise<TDBGWriteBlocksResponse>();
    auto result = promise.GetFuture();
    OnRequest(hostIndex, EOperation::WriteToDDisk);
    auto future = StorageTransport->WriteToDDisk(
        DDiskConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        NKikimr::NDDisk::TWriteInstruction(0),
        guardedSglist,
        childSpan.get());
    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         childSpan = std::move(childSpan),
         hostIndex,
         startAt,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvWriterResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [weakSelf,
                 promise = std::move(promise),
                 childSpan = std::move(childSpan),
                 hostIndex,
                 startAt,
                 threadChecker,
                 f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    const auto& response = f.GetValue();
                    NProto::TError error =
                        response.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, response.GetErrorReason());

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::WriteToDDisk,
                            error);
                    }

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
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvWritePersistentBufferResultFuture = NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>;

    const auto startAt = TMonotonic::Now();

    if (!Initialized) {
        return MakeFuture<TDBGWriteBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "Connections are not established")});
    }

    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.WriteBlocksToPBuffer");

    auto promise = NewPromise<TDBGWriteBlocksResponse>();
    auto result = promise.GetFuture();
    OnRequest(hostIndex, EOperation::WriteToPBuffer);
    auto future = StorageTransport->WriteToPBuffer(
        PBufferConnections[hostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TWriteInstruction(0),
        guardedSglist,
        childSpan.get());
    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         childSpan = std::move(childSpan),
         hostIndex,
         startAt,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TEvWritePersistentBufferResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [weakSelf,
                 promise = std::move(promise),
                 childSpan = std::move(childSpan),
                 hostIndex,
                 startAt,
                 threadChecker,
                 f]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    const auto& response = f.GetValue();
                    NProto::TError error =
                        response.GetStatus() ==
                                NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                            ? MakeError(S_OK)
                            : MakeError(E_FAIL, response.GetErrorReason());

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::WriteToPBuffer,
                            error);
                    }

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
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(hostIndexes.size() > 0);

    using TEvWriteToManyPersistentBuffersResultFuture = NThreading::TFuture<
        NTransport::IStorageTransport::TEvWriteToManyPersistentBuffersResult>;

    const auto startAt = TMonotonic::Now();

    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> disksIds;
    disksIds.reserve(hostIndexes.size());
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

    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.WriteBlocksToManyPBuffers");

    auto promise = NewPromise<TDBGWriteBlocksToManyPBuffersResponse>();
    auto result = promise.GetFuture();

    const ui8 coordinatorHostIndex = Oracle.SelectBestPBufferHost(
        hostIndexes,
        EOperation::WriteToManyPBuffers);

    OnRequest(coordinatorHostIndex, EOperation::WriteToManyPBuffers);

    auto future = StorageTransport->WriteToManyPBuffers(
        PBufferConnections[coordinatorHostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        lsn,
        NKikimr::NDDisk::TWriteInstruction(0),
        std::move(disksIds),
        replyTimeout,
        guardedSglist,
        childSpan.get());

    future.Subscribe(
        [promise = std::move(promise),
         childSpan = std::move(childSpan),
         startAt,
         coordinatorHostIndex,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         weakSelf = weak_from_this()](
            const TEvWriteToManyPersistentBuffersResultFuture& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [promise = std::move(promise),
                 childSpan = std::move(childSpan),
                 startAt,
                 coordinatorHostIndex,
                 threadChecker,
                 f,
                 weakSelf = std::move(weakSelf)]() mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());
                    if (auto self = weakSelf.lock()) {
                        self->OnWriteBlocksToManyPBuffersResponse(
                            f.GetValue(),
                            coordinatorHostIndex,
                            std::move(promise),
                            TMonotonic::Now() - startAt);
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
    ui8 coordinatorHostIndex,
    TPromise<TDBGWriteBlocksToManyPBuffersResponse> promise,
    TDuration executionTime)
{
    TDBGWriteBlocksToManyPBuffersResponse dbgResponse;
    NProto::TError coordinatorError =
        MakeError(E_FAIL, "coordinator response not found");

    for (const auto& singlePBufferResponse: response.GetResult()) {
        ui8* hostIndex = PBufferIdToHostIndex.FindPtr(
            singlePBufferResponse.GetPersistentBufferId());
        if (!hostIndex) {
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
            PBufferConnections[*hostIndex].HostConnection.DDiskId ==
            singlePBufferResponse.GetPersistentBufferId());

        NProto::TError error =
            singlePBufferResponse.GetResult().GetStatus() ==
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OK
                ? MakeError(S_OK)
                : MakeError(
                      E_FAIL,
                      singlePBufferResponse.GetResult().GetErrorReason());

        if (coordinatorHostIndex == *hostIndex) {
            coordinatorError = error;
        }

        OnResponse(
            *hostIndex,
            executionTime,
            EOperation::WriteToPBuffer,
            error);

        dbgResponse.Responses.push_back(
            {.HostIndex = *hostIndex, .Error = std::move(error)});
    }

    OnResponse(
        coordinatorHostIndex,
        executionTime,
        EOperation::WriteToManyPBuffers,
        coordinatorError);
    promise.SetValue(std::move(dbgResponse));
}

NThreading::TFuture<TDBGFlushResponse> TDirectBlockGroup::SyncWithPBuffer(
    ui32 vChunkIndex,
    ui8 pbufferHostIndex,
    ui8 ddiskHostIndex,
    const TVector<TPBufferSegment>& segments,
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    const auto startAt = TMonotonic::Now();

    TVector<NKikimr::NDDisk::TBlockSelector> selectors;
    TVector<ui64> lsns;
    for (const auto& segment: segments) {
        selectors.push_back(NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            segment.Range.Start * DefaultBlockSize,
            segment.Range.Size() * DefaultBlockSize));
        lsns.push_back(segment.Lsn);
    }

    auto childSpan = CreateChildSpan(traceId, "NbsPartition.SyncWithPBuffer");

    const auto flushOp = pbufferHostIndex == ddiskHostIndex
                             ? EOperation::Flush
                             : EOperation::FlushCrossNode;
    OnRequest(ddiskHostIndex, flushOp);

    auto future = StorageTransport->SyncWithPBuffer(
        PBufferConnections[pbufferHostIndex].HostConnection,
        DDiskConnections[ddiskHostIndex].HostConnection,
        std::move(selectors),
        std::move(lsns),
        childSpan.get());

    auto promise = NewPromise<TDBGFlushResponse>();
    auto flushFuture = promise.GetFuture();

    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         childSpan = std::move(childSpan),
         pbufferHostIndex,
         ddiskHostIndex,
         startAt,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         segmentCount = segments.size()]   //
        (const TFuture<TEvSyncWithPersistentBufferResult>& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [weakSelf,
                 promise = std::move(promise),
                 f,
                 childSpan = std::move(childSpan),
                 pbufferHostIndex,
                 ddiskHostIndex,
                 startAt,
                 segmentCount,
                 threadChecker]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    TDBGFlushResponse flushResponse;

                    if (auto self = weakSelf.lock()) {
                        flushResponse = self->HandleSyncWithPBufferResponse(
                            f.GetValue(),
                            segmentCount);
                        self->OnMultiFlushResponse(
                            pbufferHostIndex,
                            ddiskHostIndex,
                            TMonotonic::Now() - startAt,
                            flushResponse.Errors);
                    } else {
                        for (size_t i = 0; i < segmentCount; ++i) {
                            flushResponse.Errors.push_back(
                                MakeError(E_CANCELLED));
                        }
                    }

                    promise.SetValue(std::move(flushResponse));
                });
        });

    return flushFuture;
}

TDBGFlushResponse TDirectBlockGroup::HandleSyncWithPBufferResponse(
    const TEvSyncWithPersistentBufferResult& response,
    size_t segmentCount)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TDBGFlushResponse result;

    if (response.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK &&
        response.GetSegmentResults().size() == static_cast<int>(segmentCount))
    {
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
    } else {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "SyncWithPBufferResult: Segment count: %d, %d. Error %s, status: "
            "%d, error reason: %s",
            segmentCount,
            response.SegmentResultsSize(),
            response.ShortUtf8DebugString().c_str(),
            response.GetStatus(),
            response.GetErrorReason().c_str());

        for (size_t i = 0; i < segmentCount; ++i) {
            result.Errors.push_back(
                MakeError(E_FAIL, response.GetErrorReason()));
        }
    }

    return result;
}

NThreading::TFuture<TDBGEraseResponse> TDirectBlockGroup::EraseFromPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    const TVector<TPBufferSegment>& segments,
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;

    const auto startAt = TMonotonic::Now();

    TVector<NKikimr::NDDisk::TBlockSelector> selectors;
    TVector<ui64> lsns;
    for (const auto& segment: segments) {
        selectors.push_back(NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            segment.Range.Start * DefaultBlockSize,
            segment.Range.Size() * DefaultBlockSize));
        lsns.push_back(segment.Lsn);
    }

    auto childSpan = CreateChildSpan(traceId, "NbsPartition.EraseFromPBuffer");

    OnRequest(hostIndex, EOperation::Erase);

    auto future = StorageTransport->EraseFromPBuffer(
        PBufferConnections[hostIndex].HostConnection,
        std::move(selectors),
        std::move(lsns),
        childSpan.get());

    auto promise = NewPromise<TDBGEraseResponse>();
    auto result = promise.GetFuture();

    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         childSpan = std::move(childSpan),
         hostIndex,
         startAt,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         segmentCount = segments.size()]   //
        (const TFuture<TEvErasePersistentBufferResult>& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [weakSelf,
                 promise = std::move(promise),
                 childSpan = std::move(childSpan),
                 hostIndex,
                 startAt,
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

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::Erase,
                            error);
                    }

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

void TDirectBlockGroup::SetHostState(ui8 hostIndex, THostState::EState state)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "Host[%ld] state changed: %s -> %s",
        static_cast<ui64>(hostIndex),
        ToString(HostStates[hostIndex].State).c_str(),
        ToString(state).c_str());

    HostStates[hostIndex].State = state;
}

ui64 TDirectBlockGroup::GetHostPBufferUsedSize(ui8 hostIndex) const
{
    ui64 result = 0;
    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            result += vChunk->GetPBufferUsedSize(hostIndex);
        } else {
            return 0;
        }
    }
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
            "TDirectBlockGroup::OnConnectionEstablished: connection failed - "
            "unhandled error");
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

void TDirectBlockGroup::OnRequest(ui8 hostIndex, EOperation operation)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    HostStatistics[hostIndex].OnRequest(operation);
}

void TDirectBlockGroup::OnResponse(
    ui8 hostIndex,
    TDuration executionTime,
    EOperation operation,
    const NProto::TError& error)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (HasError(error)) {
        HostStatistics[hostIndex].OnError(TInstant::Now(), operation);
    } else {
        HostStatistics[hostIndex].OnSuccess(
            TInstant::Now(),
            executionTime,
            operation);
    }
}

void TDirectBlockGroup::OnMultiFlushResponse(
    ui8 pbufferHostIndex,
    ui8 ddiskHostIndex,
    TDuration executionTime,
    const TVector<NProto::TError>& errors)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    const auto operation = pbufferHostIndex == ddiskHostIndex
                               ? EOperation::Flush
                               : EOperation::FlushCrossNode;
    const bool hasError = AnyOf(
        errors,
        [](const NProto::TError& error)
        {   //
            return HasError(error);
        });

    if (hasError) {
        HostStatistics[ddiskHostIndex].OnError(TInstant::Now(), operation);
    } else {
        HostStatistics[ddiskHostIndex].OnSuccess(
            TInstant::Now(),
            executionTime,
            operation);
        HostStatistics[pbufferHostIndex].OnSuccess(
            TInstant::Now(),
            executionTime,
            operation);
    }
}

void TDirectBlockGroup::ScheduleOracleThinking()
{
    const auto delay = TDuration::MilliSeconds(
        StorageConfig->GetOracleConfig().GetThinkingInterval());

    Schedule(
        delay ? delay : DefaultOracleThinkInterval,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->Oracle.Think(TInstant::Now());
                self->ScheduleOracleThinking();
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
