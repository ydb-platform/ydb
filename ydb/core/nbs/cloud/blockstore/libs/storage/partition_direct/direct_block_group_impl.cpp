#include "direct_block_group_impl.h"

#include "partition_direct_service.h"
#include "restore_request.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/trace_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>

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

EDBGConnectionType ToDBGConnectionType(
    NTransport::THostConnection::EConnectionType connectionType)
{
    switch (connectionType) {
        case NTransport::THostConnection::EConnectionType::DDisk:
            return EDBGConnectionType::DDisk;
        case NTransport::THostConnection::EConnectionType::PBuffer:
            return EDBGConnectionType::PBuffer;
    }
    Y_ABORT("Unknown EConnectionType: %d", static_cast<int>(connectionType));
}

NProto::TError MakeDirectBlockGroupDestroyedError()
{
    return MakeError(E_REJECTED, "TDirectBlockGroup destroyed");
}

NProto::TError MakeSessionResetError()
{
    return MakeError(E_REJECTED, "DDisk session reset");
}

NProto::TError MakeSessionError(ui32 nodeId, THostIndex host)
{
    TStringBuilder result;
    result << "DDisk " << PrintHostAndNodeId(host, nodeId)
           << " session is not established";

    return MakeError(E_REJECTED, result);
}

TListPBufferResponse MakeListPBufferResponse(
    const NKikimrBlobStorage::NDDisk::TEvListPersistentBufferResult& response)
{
    TListPBufferResponse result;
    result.Error = TranslateError(response);
    result.Meta.reserve(response.GetRecords().size());
    for (const auto& segment: response.GetRecords()) {
        TPBufferKey pBufferKey{
            .Generation = segment.GetGeneration(),
            .Lsn = segment.GetLsn()};
        ui32 vChunkIndex = segment.GetSelector().GetVChunkIndex();
        auto range = TBlockRange64::WithLength(
            segment.GetSelector().GetOffsetInBytes() / DefaultBlockSize,
            segment.GetSelector().GetSize() / DefaultBlockSize);
        result.Meta.push_back(
            {.VChunkIndex = vChunkIndex,
             .PBufferKey = pBufferKey,
             .Range = range});
    }
    return result;
}

TDBGWriteBlocksToManyPBuffersResponse MakeWriteToManyPBuffersResponse(
    THostMask hosts,
    const NProto::TError& error)
{
    TDBGWriteBlocksToManyPBuffersResponse result;
    for (auto host: hosts) {
        result.Responses.push_back({.HostIndex = host, .Error = error});
    }
    return result;
}

// help function for TDirectBlockGroup::SyncWithPBuffer
std::function<void(const TFuture<NProto::TError>&)>
CreateWaitSessionCbForSyncWithPBuffer(
    TPromise<TDBGFlushResponse>&& promise,
    std::weak_ptr<TDirectBlockGroup>&& weakSelf,
    ui32 vChunkIndex,
    THostIndex pbufferHostIndex,
    THostIndex ddiskHostIndex,
    ui32 nodeId,
    const TVector<TPBufferSegment>& segments,
    std::shared_ptr<NWilson::TSpan> childSpan)
{
    using TDBGFlushResponseFuture = NThreading::TFuture<TDBGFlushResponse>;
    auto cb = [weakSelf = std::move(weakSelf),
               promise = std::move(promise),
               vChunkIndex,
               pbufferHostIndex,
               ddiskHostIndex,
               nodeId,
               segments = segments,
               childSpan = std::move(childSpan)]   //
        (const TFuture<NProto::TError>& f) mutable
    {
        TDBGFlushResponse flushResponse;
        if (HasError(f.GetValue())) {
            for (size_t i = 0; i < segments.size(); ++i) {
                flushResponse.Errors.push_back(
                    MakeSessionError(nodeId, ddiskHostIndex));
            }
            promise.SetValue(std::move(flushResponse));
            return;
        }

        if (auto self = weakSelf.lock()) {
            NWilson::TTraceId traceId;
            if (childSpan) {
                childSpan->Event("ConnectionReady");
                traceId = childSpan->GetTraceId();
            }

            self->SyncWithPBuffer(
                    vChunkIndex,
                    pbufferHostIndex,
                    ddiskHostIndex,
                    segments,
                    traceId)
                .Subscribe([promise = std::move(promise)]   //
                           (const TDBGFlushResponseFuture& f) mutable
                           { promise.SetValue(f.GetValue()); });
        } else {
            for (size_t i = 0; i < segments.size(); ++i) {
                flushResponse.Errors.push_back(
                    MakeDirectBlockGroupDestroyedError());
            }
            promise.SetValue(std::move(flushResponse));
        }
    };

    return cb;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDirectBlockGroup::TDDiskConnection::ResetSession()
{
    if (!ConnectPromise.HasValue()) {
        ConnectPromise.SetValue(MakeSessionResetError());
    }

    ConnectPromise = NThreading::NewPromise<NProto::TError>();
    ConnectFuture = ConnectPromise.GetFuture();
    SessionState = EDDiskSessionState::NotLocked;
}

const TFuture<NProto::TError>&
TDirectBlockGroup::TDDiskConnection::GetFuture() const
{
    return ConnectFuture;
}

TString TDirectBlockGroup::TDDiskConnection::DebugPrint() const
{
    TStringBuilder result;
    result << HostConnection.DebugPrint();
    auto f = GetFuture();
    if (f.IsReady()) {
        result << " c:" << FormatError(f.GetValue());
    } else {
        result << "c:<none>";
    }
    result << " s:" << ToString(SessionState);
    result << " csn:" << ConfirmedSessionSeqNo;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    NActors::TActorSystem* actorSystem,
    TStorageConfigPtr storageConfig,
    TExecutorPtr executor,
    const TDiskDescription& diskDescription,
    size_t directBlockGroupIndex,
    const TVector<NBsController::TDDiskId>& ddisksIds,
    const TVector<NBsController::TDDiskId>& pbufferIds,
    THostMask removedSlots,
    ui32 connectionConfigGeneration,
    NTransport::TStorageTransportPtr storageTransport,
    NMonitoring::TDynamicCounterPtr counters)
    : ActorSystem(actorSystem)
    , StorageConfig(std::move(storageConfig))
    , Executor(std::move(executor))
    , TabletId(diskDescription.TabletId)
    , TabletGeneration(diskDescription.Generation)
    , DirectBlockGroupIndex(directBlockGroupIndex)
    , StorageTransport(std::move(storageTransport))
    , LogTitle(
          GetCycleCount(),
          TLogTitle::TDirectBlockGroup{
              .DiskId = diskDescription.DiskId,
              .TabletId = diskDescription.TabletId,
              .Generation = diskDescription.Generation,
              .DBGIndex = DirectBlockGroupIndex})
    , ConnectionConfigGeneration(connectionConfigGeneration)
    , RemovedSlots(removedSlots)
    , Oracle(StorageConfig, this)
    , Counters(std::move(counters))
{
    Y_ASSERT(pbufferIds.size() == ddisksIds.size());
    // A remove-host can shrink the group below the default host count.
    Y_ASSERT(pbufferIds.size() >= QuorumDirectBlockGroupHostCount);

    for (THostIndex host = 0; host < ddisksIds.size(); ++host) {
        AddDDiskAndPBufferConnection(host, ddisksIds[host], pbufferIds[host]);
    }

    // A dead slot keeps its position so the numbering does not move, but it
    // has no resources left in BSC.
    for (const THostIndex slot: RemovedSlots) {
        MarkSlotDead(slot);
    }
}

TDirectBlockGroup::~TDirectBlockGroup()
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s ~TDirectBlockGroup",
        LogTitle.GetWithTime().c_str());
}

void TDirectBlockGroup::Register(TVChunkWeakPtr weakVChunk)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    // Catch a vchunk up as it registers: its config can lag the connections
    // after an add-host that committed just before a restart.
    if (auto vChunk = weakVChunk.lock()) {
        vChunk->UpdateHostCount(GetHostCount());
    }
    VChunks.push_back(std::move(weakVChunk));
}

TExecutorPtr TDirectBlockGroup::GetExecutor()
{
    return Executor;
}

ui32 TDirectBlockGroup::GetTabletGeneration() const
{
    return TabletGeneration;
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
    ITraceService* traceService,
    IPartitionDirectService* service)
{
    TraceService = traceService;
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
    using TDBGReadBlocksResponseFuture =
        NThreading::TFuture<TDBGReadBlocksResponse>;

    auto startAt = TMonotonic::Now();
    auto promise = NewPromise<TDBGReadBlocksResponse>();
    auto result = promise.GetFuture();
    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.ReadBlocks.ReadDDisk");

    if (DDiskConnections[hostIndex].SessionState != EDDiskSessionState::Locked)
    {
        if (childSpan) {
            childSpan->Event("WaitConnectionReady");
        }

        auto waitReadyCb = [weakSelf = weak_from_this(),
                            promise = std::move(promise),
                            vChunkIndex,
                            hostIndex,
                            nodeId = GetNodeId(hostIndex),
                            range,
                            guardedSglist = guardedSglist,
                            childSpan = std::move(childSpan)]   //
            (const TFuture<NProto::TError>& f) mutable
        {
            if (HasError(f.GetValue())) {
                promise.SetValue(TDBGReadBlocksResponse{
                    .Error = MakeSessionError(nodeId, hostIndex)});
                return;
            }

            if (auto self = weakSelf.lock()) {
                NWilson::TTraceId traceId;
                if (childSpan) {
                    childSpan->Event("ConnectionReady");
                    traceId = childSpan->GetTraceId();
                }

                self->ReadBlocksFromDDisk(
                        vChunkIndex,
                        hostIndex,
                        range,
                        guardedSglist,
                        traceId)
                    .Subscribe([promise = std::move(promise)]   //
                               (const TDBGReadBlocksResponseFuture& f) mutable
                               { promise.SetValue(UnsafeExtractValue(f)); });

            } else {
                promise.SetValue(TDBGReadBlocksResponse{
                    .Error = MakeDirectBlockGroupDestroyedError()});
            }
        };
        DDiskConnections[hostIndex].GetFuture().Subscribe(
            std::move(waitReadyCb));

        return result;
    }

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

                    if (auto self = weakSelf.lock()) {
                        NProto::TError error = TranslateError(f.GetValue());

                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::ReadFromDDisk,
                            true,
                            error);

                        promise.SetValue(
                            TDBGReadBlocksResponse{.Error = std::move(error)});
                    } else {
                        promise.SetValue(TDBGReadBlocksResponse{
                            .Error = MakeDirectBlockGroupDestroyedError()});
                    }
                });
        });
    return result;
}

NThreading::TFuture<TDBGReadBlocksResponse>
TDirectBlockGroup::ReadBlocksFromPBuffer(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    TPBufferKey pBufferKey,
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
        pBufferKey,
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
    using TDBGWriteBlocksResponseFuture =
        NThreading::TFuture<TDBGWriteBlocksResponse>;

    const auto startAt = TMonotonic::Now();
    auto childSpan =
        CreateChildSpan(traceId, "NbsPartition.WriteBlocksToDDisk");

    auto promise = NewPromise<TDBGWriteBlocksResponse>();
    auto result = promise.GetFuture();

    if (DDiskConnections[hostIndex].SessionState != EDDiskSessionState::Locked)
    {
        if (childSpan) {
            childSpan->Event("WaitConnectionReady");
        }

        auto waitReadyCb = [weakSelf = weak_from_this(),
                            promise = std::move(promise),
                            vChunkIndex,
                            hostIndex,
                            nodeId = GetNodeId(hostIndex),
                            range,
                            guardedSglist = guardedSglist,
                            childSpan = std::move(childSpan)]   //
            (const TFuture<NProto::TError>& f) mutable
        {
            if (HasError(f.GetValue())) {
                promise.SetValue(TDBGWriteBlocksResponse{
                    .Error = MakeSessionError(nodeId, hostIndex)});
                return;
            }
            if (auto self = weakSelf.lock()) {
                NWilson::TTraceId traceId;
                if (childSpan) {
                    childSpan->Event("ConnectionReady");
                    traceId = childSpan->GetTraceId();
                }

                self->WriteBlocksToDDisk(
                        vChunkIndex,
                        hostIndex,
                        range,
                        guardedSglist,
                        traceId)
                    .Subscribe([promise = std::move(promise)]   //
                               (const TDBGWriteBlocksResponseFuture& f) mutable
                               { promise.SetValue(f.GetValue()); });

            } else {
                promise.SetValue(TDBGWriteBlocksResponse{
                    .Error = MakeDirectBlockGroupDestroyedError()});
            }
        };
        DDiskConnections[hostIndex].GetFuture().Subscribe(
            std::move(waitReadyCb));

        return result;
    }

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

                    if (auto self = weakSelf.lock()) {
                        NProto::TError error = TranslateError(f.GetValue());

                        self->OnResponse(
                            hostIndex,
                            TMonotonic::Now() - startAt,
                            EOperation::WriteToDDisk,
                            true,
                            error);

                        promise.SetValue(
                            TDBGWriteBlocksResponse{.Error = std::move(error)});
                    } else {
                        promise.SetValue(TDBGWriteBlocksResponse{
                            .Error = MakeDirectBlockGroupDestroyedError()});
                    }
                });
        });
    return result;
}

NThreading::TFuture<TDBGWriteBlocksResponse>
TDirectBlockGroup::WriteBlocksToPBuffer(
    ui32 vChunkIndex,
    THostIndex hostIndex,
    TPBufferKey pBufferKey,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    // INVARIANT: PBuffer does NOT require a session/lock
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    // New records are always minted under the current tablet generation.
    Y_ABORT_UNLESS(pBufferKey.Generation == TabletGeneration);

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
        pBufferKey.Lsn,
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
    TPBufferKey pBufferKey,
    TBlockRange64 range,
    TDuration replyTimeout,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId,
    TWriteBlocksToManyPBuffersCallback callback)
{
    using TEvWriteToManyPersistentBuffersResult =
        NTransport::IStorageTransport::TEvWriteToManyPersistentBuffersResult;

    // INVARIANT: PBuffer does NOT require a session/lock
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(hostIndexes.Count() > 0);
    // New records are always minted under the current tablet generation.
    Y_ABORT_UNLESS(pBufferKey.Generation == TabletGeneration);

    const auto startAt = TMonotonic::Now();

    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> disksIds;
    disksIds.reserve(hostIndexes.Count());

    auto addDDisk = [&](THostIndex host)
    {
        const auto& ddiskId = PBufferConnections[host].HostConnection.DDiskId;
        disksIds.push_back({});
        ddiskId.Serialize(&disksIds.back());
    };

    // The coordinator's DDisk must be first in the request.
    addDDisk(coordinatorHostIndex);
    // The remaining DDisks follow in any order.
    for (auto host:
         hostIndexes.Exclude(THostMask::MakeOne(coordinatorHostIndex)))
    {
        addDDisk(host);
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
        (const TEvWriteToManyPersistentBuffersResult& result,
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
             result,
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
                        MakeDirectBlockGroupDestroyedError()));
                }
            });
    };

    StorageTransport->WriteToManyPBuffers(
        PBufferConnections[coordinatorHostIndex].HostConnection,
        NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            range.Start * DefaultBlockSize,
            range.Size() * DefaultBlockSize),
        pBufferKey.Lsn,
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
                "%s unexpected PBufferDiskId: %s",
                LogTitle.GetWithTime().c_str(),
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

    auto childSpan = CreateChildSpan(traceId, "NbsPartition.SyncWithPBuffer");
    auto promise = NewPromise<TDBGFlushResponse>();
    auto flushFuture = promise.GetFuture();

    if (DDiskConnections[ddiskHostIndex].SessionState !=
        EDDiskSessionState::Locked)
    {
        if (childSpan) {
            childSpan->Event("WaitConnectionReady");
        }

        auto cb = CreateWaitSessionCbForSyncWithPBuffer(
            std::move(promise),
            std::move(weak_from_this()),
            vChunkIndex,
            pbufferHostIndex,
            ddiskHostIndex,
            GetNodeId(ddiskHostIndex),
            segments,
            std::move(childSpan));
        DDiskConnections[ddiskHostIndex].GetFuture().Subscribe(std::move(cb));

        return flushFuture;
    }

    const auto startAt = TMonotonic::Now();

    TVector<NKikimr::NDDisk::TBlockSelector> selectors;
    for (const auto& segment: segments) {
        selectors.push_back(NKikimr::NDDisk::TBlockSelector(
            vChunkIndex,
            segment.Range.Start * DefaultBlockSize,
            segment.Range.Size() * DefaultBlockSize));
    }

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
        TPBufferSegment::MakePBufferKeys(segments),
        childSpan.get());

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
                            ddiskHostIndex,
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
                                MakeDirectBlockGroupDestroyedError());
                        }
                    }

                    promise.SetValue(std::move(flushResponse));
                });
        });

    return flushFuture;
}

TDBGFlushResponse TDirectBlockGroup::HandleSyncWithPBufferResponse(
    THostIndex ddiskHostIndex,
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
        NProto::TError error = TranslateError(response);
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s SyncWithPBufferResult: Segment count: %d. Response %s %s",
            LogTitle.GetWithTime().c_str(),
            segmentCount,
            response.ShortUtf8DebugString().c_str(),
            FormatError(error).Quote().c_str());

        if (IsSessionBlockedError(error)) {
            HandleBlockedGeneration(ddiskHostIndex, "SyncWithPBuffer");
        } else if (IsDeviceBrokenError(error)) {
            Oracle.OnDDiskBroken(ddiskHostIndex);
        }

        for (size_t i = 0; i < segmentCount; ++i) {
            result.Errors.push_back(error);
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
        MakePBufferKeys(segments),
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

            auto span = self->TraceService->CreateRootSpan(
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

    if (!Service->TryAdvancePBufferBarrier(
            PBufferConnections[hostIndex].HostConnection.DDiskId,
            lsn))
    {
        return;
    }

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

NThreading::TFuture<std::optional<TPBufferKey>>
TDirectBlockGroup::GatherSafeBarrierForErase()
{
    auto promise = NewPromise<std::optional<TPBufferKey>>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(), promise]() mutable
        {
            auto self = weakSelf.lock();
            if (!self) {
                promise.SetValue(std::nullopt);
                return;
            }

            std::optional<TPBufferKey> safeBarrier;
            for (const auto& weakVChunk: self->VChunks) {
                auto vChunk = weakVChunk.lock();
                if (!vChunk) {
                    continue;
                }
                const auto candidate = vChunk->GetSafeBarrierForErase();
                if (candidate && (!safeBarrier || *candidate < *safeBarrier)) {
                    safeBarrier = candidate;
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
                promise.SetValue(TDBGRestoreResponse{
                    .Error = MakeDirectBlockGroupDestroyedError()});
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
    if (RemovedSlots.Get(hostIndex)) {
        // The slot is dead: its pbuffer was drained before the removal and its
        // resources are gone in BSC, so it holds nothing to restore.
        return MakeFuture(TListPBufferResponse{});
    }

    const auto& connection = PBufferConnections[hostIndex];
    // Hold a local copy of the connect future,
    // do not put an address of changeable field into a wait.
    auto connectFuture = connection.GetFuture();
    // Switch co-routine context if needed.
    const NProto::TError& connectError = Executor->WaitFor(connectFuture);
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

void TDirectBlockGroup::OnAddHostFailed(const NProto::TError& error)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s AddHost request failed: %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(error).Quote().c_str());
}

void TDirectBlockGroup::OnAddHostSucceeded(
    THostIndex newHostIndex,
    NKikimrBlobStorage::NDDisk::TDDiskId ddiskId,
    NKikimrBlobStorage::NDDisk::TDDiskId pbufferId,
    ui32 connectionConfigGeneration)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    Y_ABORT_UNLESS(
        static_cast<size_t>(newHostIndex) == DDiskConnections.size(),
        "AddHost expects appending at the end (newHostIndex %lu vs size %lu)",
        static_cast<size_t>(newHostIndex),
        DDiskConnections.size());
    Y_ABORT_UNLESS(DDiskConnections.size() == PBufferConnections.size());
    Y_ABORT_UNLESS(DDiskConnections.size() < MaxHostCount);
    Y_ABORT_UNLESS(!DDiskConnections.empty());

    AddDDiskAndPBufferConnection(
        newHostIndex,
        NBsController::TDDiskId(ddiskId),
        NBsController::TDDiskId(pbufferId));
    ConnectionConfigGeneration = connectionConfigGeneration;

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s AddHost %s request OK, connection config generation %u",
        LogTitle.GetWithTime().c_str(),
        PrintHostAndNode(newHostIndex).c_str(),
        ConnectionConfigGeneration);

    DoEstablishConnection(newHostIndex, EConnectionType::DDisk);
    DoEstablishConnection(newHostIndex, EConnectionType::PBuffer);
}

void TDirectBlockGroup::OnRemoveHostFailed(
    THostIndex removeIndex,
    const NProto::TError& error)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s RemoveHost %s request failed: %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(removeIndex).c_str(),
        FormatError(error).c_str());
}

void TDirectBlockGroup::OnRemoveHostSucceeded(
    THostIndex removeIndex,
    ui32 connectionConfigGeneration)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(removeIndex < DDiskConnections.size());
    Y_ABORT_UNLESS(DDiskConnections.size() == PBufferConnections.size());

    MarkSlotDead(removeIndex);
    ConnectionConfigGeneration = connectionConfigGeneration;

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s RemoveHost committed: slot %s is dead",
        LogTitle.GetWithTime().c_str(),
        PrintHostAndNode(removeIndex).c_str());
}

TDuration TDirectBlockGroup::TakeCopyRangeBudget(ui64 byteCount)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return Service->TakeVolumeCopyRangeBudget(byteCount);
}

ui32 TDirectBlockGroup::GetNodeId(THostIndex host) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (DDiskConnections.size() <= host) {
        return Max<ui32>();
    }
    return DDiskConnections[host].HostConnection.DDiskId.NodeId;
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

NThreading::TFuture<TDbgSnapshot> TDirectBlockGroup::BuildMonSnapshot() const
{
    auto promise = NewPromise<TDbgSnapshot>();
    auto future = promise.GetFuture();
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         index = DirectBlockGroupIndex,
         promise = std::move(promise)]   //
        () mutable
        {
            if (auto self = weakSelf.lock()) {
                promise.SetValue(self->DoBuildMonSnapshot());
            } else {
                promise.SetValue({.Index = index});
            }
        });

    return future;
}

NThreading::TFuture<TVChunkStatsGatherResult>
TDirectBlockGroup::GatherVChunkStats(EVChunkStatsDetail detail) const
{
    auto promise = NewPromise<TVChunkStatsGatherResult>();
    auto future = promise.GetFuture();
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         detail,
         promise = std::move(promise)]   //
        () mutable
        {
            if (auto self = weakSelf.lock()) {
                promise.SetValue(self->DoGatherVChunkStats(detail));
            } else {
                promise.SetValue({});
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
        PrintHostAndNode(hostIndex).c_str(),
        ToString(oldState).c_str(),
        ToString(newState).c_str());

    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            vChunk->SetHostState(hostIndex, newState);
        }
    }
}

void TDirectBlockGroup::QueryAddHost()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(Service);

    // The position is chosen by the partition, which owns the persisted
    // membership. This request only says which state it was asked from.
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s QueryAddHost, connection config generation %u",
        LogTitle.GetWithTime().c_str(),
        ConnectionConfigGeneration);

    Service->QueryAddHost(DirectBlockGroupIndex, ConnectionConfigGeneration);
}

void TDirectBlockGroup::QueryRemoveHost(THostIndex hostIndex)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(Service);

    if (const auto reason = ValidateRemoveHost(hostIndex); !reason.empty()) {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s RemoveHost rejected (hostIndex=%s): %s",
            LogTitle.GetWithTime().c_str(),
            PrintHostAndNode(hostIndex).c_str(),
            reason.c_str());
        return;
    }

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s QueryRemoveHost %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostAndNode(hostIndex).c_str());

    Service->QueryRemoveHost(
        DirectBlockGroupIndex,
        hostIndex,
        ConnectionConfigGeneration);
}

TString TDirectBlockGroup::ValidateRemoveHost(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    const size_t slotCount = DDiskConnections.size();
    if (hostIndex >= slotCount) {
        return TStringBuilder()
               << "host index is out of range (have " << slotCount << ")";
    }
    if (RemovedSlots.Get(hostIndex)) {
        return "the slot is already removed";
    }
    const size_t liveCount = slotCount - RemovedSlots.Count();
    if (liveCount - 1 < QuorumDirectBlockGroupHostCount) {
        return TStringBuilder()
               << "removal would drop the group below the "
               << QuorumDirectBlockGroupHostCount << "-host quorum";
    }

    for (const auto& weakVChunk: VChunks) {
        auto vChunk = weakVChunk.lock();
        if (!vChunk) {
            continue;
        }
        const auto& cfg = vChunk->GetConfig();
        if (cfg.GetHostCount() != slotCount) {
            return TStringBuilder()
                   << "vchunk " << cfg.GetVChunkIndex()
                   << " config lags the connections (" << cfg.GetHostCount()
                   << " vs " << slotCount << ")";
        }
        if (!cfg.GetDisabledHosts().Get(hostIndex)) {
            return TStringBuilder() << "host is still enabled in vchunk "
                                    << cfg.GetVChunkIndex();
        }
        // Removal is irreversible, so every vchunk must keep a quorum of
        // healthy ddisks. The disabled host is not in that set already.
        const auto healthyCount = cfg.GetHealthyDDisks().Count();
        if (healthyCount < QuorumDirectBlockGroupHostCount) {
            return TStringBuilder()
                   << "vchunk " << cfg.GetVChunkIndex() << " has "
                   << healthyCount << " healthy ddisks, below the "
                   << QuorumDirectBlockGroupHostCount << "-host quorum";
        }
    }

    if (GetPBuffersUsage(hostIndex).Size != 0) {
        return "the removed host's pbuffer is not drained";
    }

    return {};
}

TCountAndSize TDirectBlockGroup::GetPBuffersUsage(THostIndex hostIndex) const
{
    TCountAndSize result;
    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            result += vChunk->GetPBuffersUsage(hostIndex);
        }
    }
    return result;
}

size_t TDirectBlockGroup::GetHostCount() const
{
    Y_ABORT_UNLESS(DDiskConnections.size() == PBufferConnections.size());
    return DDiskConnections.size();
}

void TDirectBlockGroup::AddDDiskAndPBufferConnection(
    THostIndex host,
    const NKikimr::NBsController::TDDiskId& ddiskId,
    const NKikimr::NBsController::TDDiskId& pbufferId)
{
    DDiskConnections.push_back(TDDiskConnection{
        .HostConnection = NTransport::THostConnection{
            .ConnectionType = EConnectionType::DDisk,
            .DDiskId = ddiskId,
            .Credentials = NDDisk::TQueryCredentials::ToDDisk(
                TabletId,
                TabletGeneration,
                InitialDDiskSessionSeqNo,
                std::nullopt,
                static_cast<ui32>(DirectBlockGroupIndex))}});

    PBufferConnections.push_back(TDDiskConnection{
        .HostConnection = NTransport::THostConnection{
            .ConnectionType = EConnectionType::PBuffer,
            .DDiskId = pbufferId,
            .Credentials = NDDisk::TQueryCredentials::ToPersistentBuffer(
                TabletId,
                TabletGeneration,
                std::nullopt,
                static_cast<ui32>(DirectBlockGroupIndex))}});

    NKikimrBlobStorage::NDDisk::TDDiskId id;
    pbufferId.Serialize(&id);
    const auto [_, inserted] = PBufferIdToHostIndex.insert({id, host});
    Y_ABORT_UNLESS(inserted);

    Oracle.AddHostIfNeeded(host);

    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            vChunk->UpdateHostCount(GetHostCount());
        }
    }
}

void TDirectBlockGroup::DoEstablishConnections()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (size_t i = 0; i < DDiskConnections.size(); ++i) {
        if (!RemovedSlots.Get(static_cast<THostIndex>(i))) {
            DoEstablishConnection(i, EConnectionType::DDisk);
        }
    }

    for (size_t i = 0; i < PBufferConnections.size(); ++i) {
        if (!RemovedSlots.Get(static_cast<THostIndex>(i))) {
            DoEstablishConnection(i, EConnectionType::PBuffer);
        }
    }

    DoListPBuffers();
}

void TDirectBlockGroup::DoEstablishConnection(
    THostIndex hostIndex,
    EConnectionType connectionType)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    Counters.OnConnectAttempt(ToDBGConnectionType(connectionType));

    auto& connection = connectionType == EConnectionType::DDisk
                           ? DDiskConnections[hostIndex]
                           : PBufferConnections[hostIndex];
    ui64& actualSeqNo = connection.HostConnection.Credentials.DDiskSessionSeqNo;
    if (connectionType == EConnectionType::DDisk) {
        actualSeqNo++;

        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s %s starting session: new seq_no: %lu",
            LogTitle.GetWithTime().c_str(),
            PrintHostAndNode(hostIndex).c_str(),
            actualSeqNo);
    }

    using TEvConnectResult = NKikimrBlobStorage::NDDisk::TEvConnectResult;

    auto futures = StorageTransport->Connect(connection.HostConnection);
    if (connectionType == EConnectionType::DDisk) {
        futures.DisconnectFuture.Subscribe(
            [hostIndex, weakSelf = weak_from_this(), executor = Executor]   //
            (const TFuture<ui32>& f)
            {
                executor->ExecuteSimple(
                    [hostIndex, nodeId = f.GetValue(), weakSelf]   //
                    () mutable -> void
                    {
                        if (auto self = weakSelf.lock()) {
                            self->OnNodeDisconnected(hostIndex, nodeId);
                        }
                    });
            });
    }

    futures.ConnectFuture.Subscribe(
        [weakSelf = weak_from_this(),
         executor = Executor,
         connectionType = connection.HostConnection.ConnectionType,
         hostIndex,
         actualSeqNo]   //
        (const TFuture<TEvConnectResult>& f) mutable
        {
            executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf),
                 connectionType,
                 hostIndex,
                 f,
                 actualSeqNo]   //
                () mutable
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnConnectResponse(
                            connectionType,
                            hostIndex,
                            actualSeqNo,
                            f.GetValue());
                    }
                });
        });
}

void TDirectBlockGroup::OnConnectResponse(
    EConnectionType connectionType,
    THostIndex hostIndex,
    ui64 seqNo,
    const NKikimrBlobStorage::NDDisk::TEvConnectResult& result)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TDDiskConnection& connection = connectionType == EConnectionType::DDisk
                                       ? DDiskConnections[hostIndex]
                                       : PBufferConnections[hostIndex];

    NProto::TError error = TranslateError(result);

    LOG_LOG(
        *ActorSystem,
        HasError(error) ? NActors::NLog::PRI_WARN : NActors::NLog::PRI_INFO,
        NKikimrServices::NBS_PARTITION,
        "%s OnConnectResponse: %s %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostAndNode(hostIndex).c_str(),
        FormatError(error).Quote().c_str());

    if (!HasError(error)) {
        Counters.OnConnectOk(ToDBGConnectionType(connectionType));
        if (connectionType == EConnectionType::DDisk) {
            if (seqNo <= connection.ConfirmedSessionSeqNo) {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "%s %s attempt to establish a session with an old "
                    "seq_no: %lu while actual seq_no: %lu",
                    LogTitle.GetWithTime().c_str(),
                    PrintHostAndNode(hostIndex).c_str(),
                    seqNo,
                    connection.ConfirmedSessionSeqNo);
                return;
            }
        }

        connection.HostConnection.Credentials.DDiskInstanceGuid =
            result.GetDDiskInstanceGuid();
        if (result.HasConnectionToken()) {
            auto creds = connectionType == EConnectionType::DDisk
                             ? NDDisk::TQueryCredentials::ToDDisk(
                                   result.GetConnectionToken())
                             : NDDisk::TQueryCredentials::ToPersistentBuffer(
                                   result.GetConnectionToken());
            connection.HostConnection.Credentials.ConnectionToken =
                creds.ConnectionToken;
        } else {
            connection.HostConnection.Credentials.ConnectionToken =
                std::nullopt;
        }

        if (connectionType == EConnectionType::DDisk) {
            connection.SessionState = EDDiskSessionState::Locked;
            connection.ConfirmedSessionSeqNo = seqNo;
            Oracle.OnDDiskConnected(hostIndex, TInstant::Now());
        }
        // INVARIANT: PBuffer does NOT require a session/lock
    } else if (IsSessionBlockedError(error)) {
        Counters.OnConnectErr(ToDBGConnectionType(connectionType));
        // Terminal: our tablet generation is stale. Suicide, no reconnect.
        HandleBlockedGeneration(hostIndex, "Connect");
        // Unblock waiters on ConnectFuture with the error.
        connection.ConnectPromise.SetValue(error);
        return;
    } else {
        Counters.OnConnectErr(ToDBGConnectionType(connectionType));
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s connection failed %s: %s",
            LogTitle.GetWithTime().c_str(),
            PrintHostAndNode(hostIndex).c_str(),
            FormatError(error).Quote().c_str());
        ReEstablishConnection(connectionType, hostIndex);
        return;
    }

    // ConnectPromise resolves both "connection ready" and "session ready" in
    // this phase. Unblocks waiters in ReadFromDDisk/WriteToDDisk/ListPBuffers.
    connection.ConnectPromise.SetValue(error);
    if (!IsInitialized() && HasLockedQuorum() && HasPBufferQuorum()) {
        InitialReadyPromise.SetValue();
        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s DBG reached initial locked quorum (>= %zu sessions)",
            LogTitle.GetWithTime().c_str(),
            MinLockedDDiskSessionsToStart);
    }
}

void TDirectBlockGroup::ReEstablishConnection(
    EConnectionType connectionType,
    THostIndex hostIndex)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    auto& connections = connectionType == EConnectionType::DDisk
                            ? DDiskConnections
                            : PBufferConnections;
    Y_ABORT_UNLESS(hostIndex < connections.size());
    TDDiskConnection& connection = connections[hostIndex];

    if (RemovedSlots.Get(hostIndex)) {
        return;   // nothing to reconnect to, the resources are deleted
    }

    Counters.OnReconnect(ToDBGConnectionType(connectionType));

    if (BlockedGenerationDetected) {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s reconnect %s suppressed: blocked generation, suicide in "
            "progress",
            LogTitle.GetWithTime().c_str(),
            PrintHostAndNode(hostIndex).c_str());
        return;
    }

    connection.ResetSession();
    TDuration reconnectDelay = Oracle.GetHostReconnectDelay(hostIndex);
    Schedule(
        reconnectDelay,
        [hostIndex, weakSelf = weak_from_this(), connectionType]()
        {
            if (auto self = weakSelf.lock()) {
                self->DoEstablishConnection(hostIndex, connectionType);
            }
        });
}

void TDirectBlockGroup::OnNodeDisconnected(THostIndex hostIndex, ui32 nodeId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnNodeDisconnected %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostAndNodeId(hostIndex, nodeId).c_str());

    Counters.OnDisconnect(EDBGConnectionType::DDisk);
    Oracle.OnDDiskDisconnected(hostIndex, TInstant::Now());

    // OnNodeDisconnected may be called only for DDisk
    ReEstablishConnection(EConnectionType::DDisk, hostIndex);
}

void TDirectBlockGroup::MarkSlotDead(THostIndex slot)
{
    Y_ABORT_UNLESS(slot < PBufferConnections.size());

    RemovedSlots.Set(slot);
    Oracle.OnHostRemoved(slot);

    // BSC may grant the freed pbuffer id to a future host, so the map must not
    // keep pointing the old id at this slot.
    NKikimrBlobStorage::NDDisk::TDDiskId pbufferId;
    PBufferConnections[slot].HostConnection.DDiskId.Serialize(&pbufferId);
    PBufferIdToHostIndex.erase(pbufferId);
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
                {.PBufferKey = meta.PBufferKey,
                 .Range = meta.Range,
                 .HostIndex = hostIndex});
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
            PrintHostAndNode(hostIndex).c_str(),
            ToString(operation).c_str(),
            FormatError(error).Quote().c_str());

        if (IsCanNotAcquireDataError(error)) {
            Oracle.OnRequestCancelled(hostIndex, operation, TInstant::Now());
        } else if (IsDDiskOperation(operation) && IsSessionBlockedError(error))
        {
            HandleBlockedGeneration(hostIndex, ToString(operation));
        } else if (IsDeviceBrokenError(error)) {
            Oracle.OnDDiskBroken(hostIndex);
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
            return IsCanNotAcquireDataError(error);
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

void TDirectBlockGroup::HandleBlockedGeneration(
    THostIndex hostIndex,
    TStringBuf context)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (BlockedGenerationDetected) {
        return;
    }
    BlockedGenerationDetected = true;

    DDiskConnections[hostIndex].SessionState = EDDiskSessionState::Broken;
    const TString reason =
        TStringBuilder() << "dbg:" << DirectBlockGroupIndex << " "
                         << PrintHostAndNode(hostIndex)
                         << " DDisk returned BLOCKED (stale tablet generation) "
                         << context;

    LOG_ERROR(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s SUICIDE: %s",
        LogTitle.GetWithTime().c_str(),
        reason.c_str());

    // No retry/reconnect: signal the actor to suicide.
    Service->StopTablet(reason);
}

TDBGDumpResponse TDirectBlockGroup::DoDebugPrintDirtyMap() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TStringBuilder sb;
    sb << "DBG[" << DirectBlockGroupIndex << "]\n";

    for (const auto& conn: DDiskConnections) {
        sb << " " << conn.DebugPrint() << "\n";
    }
    for (const auto& conn: PBufferConnections) {
        sb << " " << conn.DebugPrint() << "\n";
    }

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

TDbgSnapshot TDirectBlockGroup::DoBuildMonSnapshot() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TVector<TConnectionSnapshot> connections;
    connections.reserve(DDiskConnections.size());
    for (size_t host = 0; host < DDiskConnections.size(); ++host) {
        connections.push_back(MakeConnectionSnapshot(host));
    }

    auto hostsStat = Oracle.BuildHostStats(TInstant::Now());
    TVChunkConfigs vChunkConfigs;
    for (const auto& weakVChunk: VChunks) {
        if (auto vChunk = weakVChunk.lock()) {
            vChunkConfigs[vChunk->GetConfig().GetVChunkIndex()] =
                vChunk->GetConfig();

            for (THostIndex host = 0; host < GetHostCount(); ++host) {
                hostsStat[host].AheadBlocks += vChunk->GetAheadBlocks(host);
                hostsStat[host].BehindBlocks += vChunk->GetBehindBlocks(host);
            }
        }
    }

    return {
        .Index = DirectBlockGroupIndex,
        .VChunkCount = VChunks.size(),
        .Hosts = std::move(hostsStat),
        .Connections = std::move(connections),
        .VChunkConfigs = std::move(vChunkConfigs),
        .LatencyHistoryCapacity = Oracle.GetLatencyHistoryCapacity(),
    };
}

TVChunkStatsGatherResult TDirectBlockGroup::DoGatherVChunkStats(
    EVChunkStatsDetail detail) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TVChunkStatsGatherResult result;
    result.DbgIndex = DirectBlockGroupIndex;
    if (detail == EVChunkStatsDetail::PerVChunk) {
        result.PerVChunk.reserve(VChunks.size());
    }
    for (const auto& weakVChunk: VChunks) {
        auto vChunk = weakVChunk.lock();
        if (!vChunk) {
            continue;
        }
        const TVChunkStats& stats = vChunk->GetStats();
        result.Total.Accumulate(stats);
        if (detail == EVChunkStatsDetail::PerVChunk) {
            result.PerVChunk.push_back(TVChunkStatsSnapshot{
                .VChunkIndex = vChunk->GetConfig().GetVChunkIndex(),
                .DbgIndex = DirectBlockGroupIndex,
                .Stats = stats,
            });
        }
    }
    return result;
}

TConnectionSnapshot TDirectBlockGroup::MakeConnectionSnapshot(
    size_t hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(DDiskConnections.size() == PBufferConnections.size());

    const auto& ddisk = DDiskConnections[hostIndex];
    const auto& pbuffer = PBufferConnections[hostIndex];

    return {
        .HostIndex = static_cast<THostIndex>(hostIndex),
        .DDiskId = ddisk.HostConnection.DDiskId,
        .PBufferId = pbuffer.HostConnection.DDiskId,
        .DDiskSession = ToString(ddisk.SessionState),
        .DDiskConnected = ddisk.ConnectPromise.HasValue(),
        .PBufferConnected = pbuffer.ConnectPromise.HasValue(),
    };
}

TString TDirectBlockGroup::PrintHostAndNode(THostIndex host) const
{
    return PrintHostAndNodeId(host, GetNodeId(host));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
