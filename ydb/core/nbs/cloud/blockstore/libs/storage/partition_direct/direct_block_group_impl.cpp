#include "direct_block_group_impl.h"

#include "restore_request.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>
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
constexpr ui64 InitialDDiskSessionSeqNo = 0;

constexpr size_t MinLockedDDiskSessionsToStart =
    QuorumDirectBlockGroupHostCount;

////////////////////////////////////////////////////////////////////////////////

TListPBufferResponse MakeListPBufferResponse(
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& response)
{
    TListPBufferResponse result;
    result.Error = TranslateError(response);
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

TDBGWriteBlocksToManyPBuffersResponse MakeWriteToManyPBuffersResponse(
    THostMask hosts,
    EWellKnownResultCodes code,
    const TString& reason)
{
    TDBGWriteBlocksToManyPBuffersResponse result;
    for (auto host: hosts) {
        result.Responses.push_back(
            {.HostIndex = host, .Error = MakeError(code, reason)});
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
    TExecutorPtr executor,
    const TString& diskId,
    ui64 tabletId,
    ui32 generation,
    size_t directBlockGroupIndex,
    const TVector<NBsController::TDDiskId>& ddisksIds,
    const TVector<NBsController::TDDiskId>& pbufferIds,
    std::unique_ptr<NTransport::IStorageTransport> storageTransport)
    : ActorSystem(actorSystem)
    , StorageConfig(std::move(storageConfig))
    , Executor(std::move(executor))
    , TabletId(tabletId)
    , TabletGeneration(generation)
    , DirectBlockGroupIndex(directBlockGroupIndex)
    , StorageTransport(std::move(storageTransport))
    , LogTitle(
          GetCycleCount(),
          TLogTitle::TDirectBlockGroup{
              .DiskId = diskId,
              .TabletId = TabletId,
              .Generation = TabletGeneration,
              .DirectBlockGroupIndex = DirectBlockGroupIndex,
          })
    , Oracle(StorageConfig, this)
{
    Y_ASSERT(pbufferIds.size() == DirectBlockGroupHostCount);
    Y_ASSERT(ddisksIds.size() == DirectBlockGroupHostCount);

    auto addDDiskConnections = [&](const TVector<NBsController::TDDiskId>& ids,
                                   TVector<TDDiskConnection>& connections,
                                   EConnectionType type)
    {
        for (THostIndex i = 0; i < ids.size(); ++i) {
            const auto& ddiskId = ids[i];
            const auto credentials =
                type == EConnectionType::PBuffer
                    ? NDDisk::TQueryCredentials::ToPersistentBuffer(
                          TabletId,
                          generation,
                          std::nullopt)
                    : NDDisk::TQueryCredentials::ToDDisk(
                          TabletId,
                          generation,
                          InitialDDiskSessionSeqNo,
                          std::nullopt);
            connections.push_back(TDDiskConnection{
                .HostConnection = NTransport::THostConnection{
                    .ConnectionType = type,
                    .DDiskId = ddiskId,
                    .Credentials = credentials}});

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

void TDirectBlockGroup::Register(TVChunkWeakPtr vChunk)
{
    VChunks.push_back(std::move(vChunk));
}

TExecutorPtr TDirectBlockGroup::GetExecutor()
{
    return Executor;
}

IOraclePtr TDirectBlockGroup::GetOracle()
{
    return &Oracle;
}

void TDirectBlockGroup::Schedule(TDuration delay, TCallback callback)
{
    Y_ABORT_UNLESS(Service);

    Service->ScheduleAfterDelay(Executor, delay, std::move(callback));
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

NThreading::TFuture<void> TDirectBlockGroup::Run(
    IPartitionDirectService* service)
{
    Service = service;

    ScheduleOracleThinking();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this()]   //
        ()
        {
            if (auto self = weakSelf.lock()) {
                self->DoEstablishConnections();
            }
        });

    return InitialReadyPromise.GetFuture();
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroup::ReadBlocksFromDDisk(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvReadResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadResult>;

    if (!WaitForSessionLock(hostIndex)) {
        return MakeFuture<TDBGReadBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "DDisk session is not established")});
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

                    NProto::TError error = TranslateError(f.GetValue());
                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::ReadFromDDisk,
                            true,
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
    THostIndex hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    // INVARIANT: PBuffer does NOT require a session/lock
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvReadPersistentBufferResultFuture =
        TFuture<NKikimrBlobStorage::NDDisk::TEvReadPersistentBufferResult>;

    const auto startAt = TMonotonic::Now();

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

                    NProto::TError error = TranslateError(f.GetValue());

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::ReadFromPBuffer,
                            true,
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
    THostIndex hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvWriterResultFuture =
        NThreading::TFuture<NKikimrBlobStorage::NDDisk::TEvWriteResult>;

    const auto startAt = TMonotonic::Now();

    if (!WaitForSessionLock(hostIndex)) {
        return MakeFuture<TDBGWriteBlocksResponse>(
            {.Error =
                 MakeError(E_REJECTED, "DDisk session is not established")});
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

                    NProto::TError error = TranslateError(f.GetValue());

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::WriteToDDisk,
                            true,
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
    THostIndex hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    // INVARIANT: PBuffer does NOT require a session/lock
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvWritePersistentBufferResultFuture = NThreading::TFuture<
        NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult>;

    const auto startAt = TMonotonic::Now();

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

                    NProto::TError error = TranslateError(f.GetValue());

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::WriteToPBuffer,
                            true,
                            error);
                    }

                    promise.SetValue(
                        TDBGWriteBlocksResponse{.Error = std::move(error)});
                });
        });
    return result;
}

void TDirectBlockGroup::WriteBlocksToManyPBuffers(
    ui32 vChunkIndex,
    THostIndex coordinatorHostIndex,
    THostMask hostIndexes,
    ui64 lsn,
    TBlockRange64 range,
    TDuration replyTimeout,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId,
    TWriteBlocksToManyPBuffersCallback callback)
{
    // INVARIANT: PBuffer does NOT require a session/lock
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(hostIndexes.Count() > 0);

    const auto startAt = TMonotonic::Now();

    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> disksIds;
    disksIds.reserve(hostIndexes.Count());

    for (auto hostIndex: hostIndexes) {
        const auto& ddiskId =
            PBufferConnections[hostIndex].HostConnection.DDiskId;

        disksIds.push_back({});
        ddiskId.Serialize(&disksIds.back());
    }

    OnRequest(coordinatorHostIndex, EOperation::WriteToManyPBuffers);

    auto writeToManyPBuffersCB =
        [startAt,
         coordinatorHostIndex,
         hostIndexes,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate(),
         callback = std::move(callback),
         weakSelf = weak_from_this()]   //
        (NTransport::IStorageTransport::TEvWriteToManyPersistentBuffersResult
             result,
         std::shared_ptr<NWilson::TSpan> span) mutable
    {
        // ActorSystem thread
        auto responseSpan =
            span ? std::make_shared<NWilson::TSpan>(span->CreateChild(
                       NKikimr::TWilsonNbs::NbsBasic,
                       "WriteBlocksToManyPBuffers.Response",
                       NWilson::EFlags::AUTO_END))
                 : nullptr;
        executor->ExecuteSimple(
            [responseSpan = std::move(responseSpan),
             startAt,
             coordinatorHostIndex,
             hostIndexes,
             threadChecker,
             result = std::move(result),
             callback,
             weakSelf]() mutable -> void
            {
                Y_ABORT_UNLESS(threadChecker.Check());
                if (responseSpan) {
                    responseSpan->Event("Reply on DBG thread");
                }

                if (auto self = weakSelf.lock()) {
                    self->OnWriteBlocksToManyPBuffersResponse(
                        result,
                        coordinatorHostIndex,
                        std::move(callback),
                        TMonotonic::Now() - startAt);
                } else {
                    callback(MakeWriteToManyPBuffersResponse(
                        hostIndexes,
                        E_CANCELLED,
                        "DBG is destroyed"));
                }
            });
    };

    StorageTransport->WriteToManyPBuffers(
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
        CreateChildSpan(traceId, "NbsPartition.WriteBlocksToManyPBuffers"),
        std::move(writeToManyPBuffersCB));
}

void TDirectBlockGroup::OnWriteBlocksToManyPBuffersResponse(
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffersResult& response,
    THostIndex coordinatorHostIndex,
    TWriteBlocksToManyPBuffersCallback callback,
    TDuration executionTime)
{
    TDBGWriteBlocksToManyPBuffersResponse dbgResponse;

    bool coordinatorFound = false;
    for (const auto& singlePBufferResponse: response.GetResult()) {
        const THostIndex* const hostIndex = PBufferIdToHostIndex.FindPtr(
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
            TranslateError(singlePBufferResponse.GetResult());

        const bool isCoordinator = coordinatorHostIndex == *hostIndex;
        coordinatorFound = coordinatorFound || isCoordinator;

        OnResponse(
            *hostIndex,
            executionTime,
            isCoordinator ? EOperation::WriteToManyPBuffers
                          : EOperation::WriteToPBuffer,
            isCoordinator,
            error);

        dbgResponse.Responses.push_back(
            {.HostIndex = *hostIndex, .Error = std::move(error)});
    }

    if (!coordinatorFound) {
        Oracle.OnRequestCancelled(
            coordinatorHostIndex,
            EOperation::WriteToManyPBuffers,
            TInstant::Now());
    }

    callback(std::move(dbgResponse));
}

NThreading::TFuture<TDBGFlushResponse> TDirectBlockGroup::SyncWithPBuffer(
    ui32 vChunkIndex,
    THostIndex pbufferHostIndex,
    THostIndex ddiskHostIndex,
    const TVector<TPBufferSegment>& segments,
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!WaitForSessionLock(ddiskHostIndex)) {
        TDBGFlushResponse rejectResponse;
        rejectResponse.Errors.reserve(segments.size());
        for (size_t i = 0; i < segments.size(); ++i) {
            rejectResponse.Errors.push_back(
                MakeError(E_REJECTED, "DDisk session is not established"));
        }
        return MakeFuture<TDBGFlushResponse>(std::move(rejectResponse));
    }

    const auto startAt = TMonotonic::Now();

    TVector<NKikimr::NDDisk::TBlockSelector> selectors;
    for (const auto& segment: segments) {
        selectors.push_back(NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            segment.Range.Start * DefaultBlockSize,
            segment.Range.Size() * DefaultBlockSize));
    }

    auto childSpan = CreateChildSpan(traceId, "NbsPartition.SyncWithPBuffer");

    if (pbufferHostIndex == ddiskHostIndex) {
        OnRequest(ddiskHostIndex, EOperation::Flush);
    } else {
        OnRequest(pbufferHostIndex, EOperation::FlushCrossNode);
        OnRequest(ddiskHostIndex, EOperation::FlushCrossNode);
    }

    auto future = StorageTransport->SyncWithPBuffer(
        PBufferConnections[pbufferHostIndex].HostConnection,
        DDiskConnections[ddiskHostIndex].HostConnection,
        std::move(selectors),
        TPBufferSegment::MakeLsnVector(segments),
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
        (const TFuture<TEvSyncResult>& f) mutable
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
    const TEvSyncResult& response,
    size_t segmentCount)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TDBGFlushResponse result;

    if (HasSuccess(response) &&
        response.GetSegmentResults().size() == static_cast<int>(segmentCount))
    {
        for (size_t i = 0; i < segmentCount; ++i) {
            const auto& segmentResult = response.GetSegmentResults(i);
            result.Errors.push_back(TranslateError(
                segmentResult,
                ETranslateFlags::TreatOutdatedAsSuccess));
        }
    } else {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s SyncWithPBufferResult: Segment count: %d. Response %s %s",
            LogTitle.GetWithTime().c_str(),
            segmentCount,
            response.ShortUtf8DebugString().c_str(),
            FormatError(TranslateError(response)).c_str());

        for (size_t i = 0; i < segmentCount; ++i) {
            result.Errors.push_back(
                MakeError(E_FAIL, response.GetErrorReason()));
        }
    }

    return result;
}

NThreading::TFuture<TDBGEraseResponse> TDirectBlockGroup::BatchEraseFromPBuffer(
    THostIndex hostIndex,
    const TEraseSegments& segments,
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;

    const auto startAt = TMonotonic::Now();

    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.BatchEraseFromPBuffer");

    OnRequest(hostIndex, EOperation::Erase);

    auto future = StorageTransport->BatchEraseFromPBuffer(
        PBufferConnections[hostIndex].HostConnection,
        MakeLsnVector(segments),
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

                    NProto::TError error = TranslateError(result);

                    if (auto self = weakSelf.lock()) {
                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::Erase,
                            true,
                            error);
                    }

                    promise.SetValue(
                        TDBGEraseResponse{.Error = std::move(error)});
                });
        });

    return result;
}

void TDirectBlockGroup::BarrierEraseFromPBuffer(ui64 lsn)
{
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(), lsn]()
        {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }
            LOG_DEBUG(
                *self->ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s barrier-erase lsn=%lu on %lu PBuffer hosts",
                self->LogTitle.GetWithTime().c_str(),
                lsn,
                self->PBufferConnections.size());

            auto span = self->Service->CreteRootSpan(
                "NbsPartition.BarrierEraseFromPBuffer");

            for (THostIndex h = 0; h < self->PBufferConnections.size(); ++h) {
                self->DoBarrierEraseFromPBuffer(h, lsn, span.GetTraceId());
            }
        });
}

void TDirectBlockGroup::DoBarrierEraseFromPBuffer(
    THostIndex hostIndex,
    ui64 lsn,
    const NWilson::TTraceId& traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    using TEvErasePersistentBufferResult =
        NKikimrBlobStorage::NDDisk::TEvErasePersistentBufferResult;

    const auto startAt = TMonotonic::Now();

    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.DoBarrierEraseFromPBuffer");

    OnRequest(hostIndex, EOperation::BarrierErase);

    auto future = StorageTransport->BarrierEraseFromPBuffer(
        PBufferConnections[hostIndex].HostConnection,
        lsn,
        childSpan.get());

    future.Subscribe(
        [weakSelf = weak_from_this(),
         childSpan = std::move(childSpan),
         hostIndex,
         startAt,
         executor = Executor,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TFuture<TEvErasePersistentBufferResult>& f) mutable
        {
            // ActorSystem thread

            executor->ExecuteSimple(
                [weakSelf,
                 childSpan = std::move(childSpan),
                 hostIndex,
                 startAt,
                 threadChecker,
                 result = UnsafeExtractValue(f)]   //
                () mutable
                {
                    Y_ABORT_UNLESS(threadChecker.Check());

                    auto self = weakSelf.lock();
                    if (!self) {
                        return;
                    }
                    self->OnResponse(
                        hostIndex,
                        TMonotonic::Now() - startAt,
                        EOperation::BarrierErase,
                        true,
                        TranslateError(result));
                });
        });
}

NThreading::TFuture<std::optional<ui64>>
TDirectBlockGroup::GatherSafeBarrierForErase()
{
    auto promise = NewPromise<std::optional<ui64>>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(), promise]() mutable
        {
            auto self = weakSelf.lock();
            if (!self) {
                promise.SetValue(std::nullopt);
                return;
            }

            std::optional<ui64> safeBarrier;
            for (const auto& weakVChunk: self->VChunks) {
                auto vChunk = weakVChunk.lock();
                if (!vChunk) {
                    continue;
                }
                const auto lsn = vChunk->GetSafeBarrierForErase();
                if (lsn && (!safeBarrier || *lsn < *safeBarrier)) {
                    safeBarrier = lsn;
                }
            }
            promise.SetValue(safeBarrier);
        });

    return future;
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
    THostIndex hostIndex)
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

NThreading::TFuture<TDBGDumpResponse> TDirectBlockGroup::Dump()
{
    auto promise = NewPromise<TDBGDumpResponse>();
    auto future = promise.GetFuture();
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         index = DirectBlockGroupIndex,
         promise = std::move(promise)]   //
        () mutable
        {
            if (auto self = weakSelf.lock()) {
                promise.SetValue(self->DoDebugPrintDirtyMap());
            } else {
                promise.SetValue({.DirectBlockGroupIndex = index});
            }
        });

    return future;
}

void TDirectBlockGroup::SetHostState(
    THostIndex hostIndex,
    EHostState oldState,
    EHostState newState)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s %s state changed: %s -> %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(hostIndex).c_str(),
        ToString(oldState).c_str(),
        ToString(newState).c_str());

    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            vChunk->SetHostState(hostIndex, newState);
        }
    }
}

ui64 TDirectBlockGroup::GetHostPBufferUsedSize(THostIndex hostIndex) const
{
    ui64 result = 0;
    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            result += vChunk->GetPBufferUsedSize(hostIndex);
        }
    }
    return result;
}

ui64 TDirectBlockGroup::GetDDiskSessionSeqNo(size_t index) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(index < DDiskConnections.size());
    return DDiskConnections[index].ConfirmedSessionSeqNo;
}

void TDirectBlockGroup::DoEstablishConnections()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (size_t i = 0; i < DDiskConnections.size(); ++i) {
        DoEstablishConnection(i, EConnectionType::DDisk);
    }

    for (size_t i = 0; i < PBufferConnections.size(); ++i) {
        DoEstablishConnection(i, EConnectionType::PBuffer);
    }

    DoListPBuffers();
}

void TDirectBlockGroup::DoEstablishConnection(
    size_t index,
    EConnectionType connectionType)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto& connection = connectionType == EConnectionType::DDisk
                           ? DDiskConnections[index]
                           : PBufferConnections[index];
    ui64& actualSeqNo = connection.HostConnection.Credentials.DDiskSessionSeqNo;
    if (connectionType == EConnectionType::DDisk) {
        actualSeqNo++;

        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s host[%zu] starting session: new seq_no: %lu ",
            LogTitle.GetWithTime().c_str(),
            index,
            actualSeqNo);
    }

    using TEvConnectResult = NKikimrBlobStorage::NDDisk::TEvConnectResult;

    auto future = StorageTransport->Connect(connection.HostConnection);

    future.Subscribe(
        [weakSelf = weak_from_this(),
         executor = Executor,
         connectionType = connection.HostConnection.ConnectionType,
         index,
         actualSeqNo]   //
        (const TFuture<TEvConnectResult>& f) mutable
        {
            executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf),
                 connectionType,
                 index,
                 f,
                 actualSeqNo]   //
                () mutable
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnConnectionEstablished(
                            connectionType,
                            index,
                            actualSeqNo,
                            f.GetValue());
                    }
                });
        });
}

void TDirectBlockGroup::OnConnectionEstablished(
    EConnectionType connectionType,
    size_t index,
    ui64 seqNo,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TDDiskConnection& connection = connectionType == EConnectionType::DDisk
                                       ? DDiskConnections[index]
                                       : PBufferConnections[index];

    NProto::TError error = TranslateError(result);
    if (!HasError(error)) {
        connection.HostConnection.Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
        if (connectionType == EConnectionType::DDisk) {
            if (seqNo <= connection.ConfirmedSessionSeqNo) {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "%s host[%zu] attempt to establish a session with an old "
                    "seq_no: %lu while actual seq_no: %lu ",
                    LogTitle.GetWithTime().c_str(),
                    index,
                    seqNo,
                    connection.ConfirmedSessionSeqNo);
                return;
            }
            connection.SessionState = EDDiskSessionState::Locked;
            connection.ConfirmedSessionSeqNo = seqNo;
        }
        // INVARIANT: PBuffer does NOT require a session/lock
    } else {
        // TODO (future phase): handle the error code/BLOCKED, transition to
        // Broken/suicide.
        Y_ABORT(
            "TDirectBlockGroup::OnConnectionEstablished: connection failed - "
            "unhandled error");
    }

    // ConnectPromise resolves both "connection ready" and "session ready" in
    // this phase. Unblocks waiters in ReadFromDDisk/WriteToDDisk/ListPBuffers.
    connection.ConnectPromise.SetValue(error);
    if (!InitialReadyPromise.HasValue() && HasLockedQuorum() &&
        HasPBufferQuorum())
    {
        InitialReadyPromise.SetValue();
        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s DBG reached initial locked quorum (>= %zu sessions)",
            LogTitle.GetWithTime().c_str(),
            MinLockedDDiskSessionsToStart);
    }
}

bool TDirectBlockGroup::HasPBufferQuorum() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    size_t sessionsEstablishedCount = 0;
    for (const auto& c: PBufferConnections) {
        if (c.ConnectPromise.HasValue()) {
            ++sessionsEstablishedCount;
        }
    }
    return sessionsEstablishedCount >= QuorumDirectBlockGroupHostCount;
}

bool TDirectBlockGroup::HasLockedQuorum() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    size_t lockedCount = 0;
    for (const auto& c: DDiskConnections) {
        if (c.SessionState == EDDiskSessionState::Locked) {
            ++lockedCount;
        }
    }
    return lockedCount >= MinLockedDDiskSessionsToStart;
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

void TDirectBlockGroup::OnRequest(THostIndex hostIndex, EOperation operation)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    Oracle.OnRequestStarted(hostIndex, operation, TInstant::Now());
}

void TDirectBlockGroup::OnResponse(
    THostIndex hostIndex,
    TDuration executionTime,
    EOperation operation,
    bool needDecreaseInflightCounters,
    const NProto::TError& error)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!needDecreaseInflightCounters) {
        Oracle.OnRequestStarted(hostIndex, operation, TInstant::Now());
    }

    if (HasError(error)) {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OnResponse %s %s %s",
            LogTitle.GetWithTime().c_str(),
            PrintHostIndex(hostIndex).c_str(),
            ToString(operation).c_str(),
            FormatError(error).c_str());

        if (IsCancelledError(error)) {
            Oracle.OnRequestCancelled(hostIndex, operation, TInstant::Now());
        } else {
            Oracle.OnRequestFailed(hostIndex, operation, TInstant::Now());
        }
    } else {
        Oracle.OnRequestSucceeded(
            hostIndex,
            operation,
            TInstant::Now(),
            executionTime);
    }
}

void TDirectBlockGroup::OnMultiFlushResponse(
    THostIndex pbufferHostIndex,
    THostIndex ddiskHostIndex,
    TDuration executionTime,
    const TVector<NProto::TError>& errors)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    const auto now = TInstant::Now();

    const bool hasError = AnyOf(
        errors,
        [](const NProto::TError& error)
        {   //
            return HasError(error);
        });
    const bool cancelled = AllOf(
        errors,
        [](const NProto::TError& error)
        {   //
            return IsCancelledError(error);
        });

    if (cancelled) {
        if (pbufferHostIndex == ddiskHostIndex) {
            Oracle.OnRequestCancelled(pbufferHostIndex, EOperation::Flush, now);
        } else {
            Oracle.OnRequestCancelled(
                ddiskHostIndex,
                EOperation::FlushCrossNode,
                now);
            Oracle.OnRequestCancelled(
                pbufferHostIndex,
                EOperation::FlushCrossNode,
                now);
        }
        return;
    }

    if (hasError) {
        if (pbufferHostIndex == ddiskHostIndex) {
            Oracle.OnRequestFailed(pbufferHostIndex, EOperation::Flush, now);
        } else {
            // Count error only for ddiskHostIndex due-to pull model
            Oracle.OnRequestFailed(
                ddiskHostIndex,
                EOperation::FlushCrossNode,
                now);
            Oracle.OnRequestCancelled(
                pbufferHostIndex,
                EOperation::FlushCrossNode,
                now);
        }
        return;
    }

    // OK
    if (pbufferHostIndex == ddiskHostIndex) {
        Oracle.OnRequestSucceeded(
            pbufferHostIndex,
            EOperation::Flush,
            now,
            executionTime);
    } else {
        Oracle.OnRequestSucceeded(
            ddiskHostIndex,
            EOperation::FlushCrossNode,
            now,
            executionTime);
        Oracle.OnRequestSucceeded(
            pbufferHostIndex,
            EOperation::FlushCrossNode,
            now,
            executionTime);
    }
}

void TDirectBlockGroup::Thinking()
{
    Oracle.Think(TInstant::Now());
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
                self->Thinking();
                self->ScheduleOracleThinking();
            }
        });
}

bool TDirectBlockGroup::WaitForSessionLock(THostIndex hostIndex)
{
    const auto& conn = DDiskConnections[hostIndex];
    if (conn.SessionState != EDDiskSessionState::Locked) {
        const auto sessionReadyFuture = conn.GetFuture();
        Executor->WaitFor(sessionReadyFuture);
        return conn.SessionState == EDDiskSessionState::Locked;
    }
    return true;
}

TDBGDumpResponse TDirectBlockGroup::DoDebugPrintDirtyMap()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TStringBuilder sb;
    sb << "DBG[" << DirectBlockGroupIndex << "]\n";
    sb << Oracle.Dump();

    TDBGDumpResponse result;
    result.DirectBlockGroupIndex = DirectBlockGroupIndex;
    result.Dump = std::move(sb);
    result.Dumps.reserve(VChunks.size());
    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            result.Dumps.push_back(
                {.VChunkConfig = vChunk->GetConfig(),
                 .Dump = vChunk->DebugPrintDirtyMap()});
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
