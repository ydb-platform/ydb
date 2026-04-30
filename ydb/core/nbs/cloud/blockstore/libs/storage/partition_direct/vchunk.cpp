#include "vchunk.h"

#include "flush_request.h"
#include "range_translate.h"
#include "read_request_executor.h"
#include "write_with_direct_replication_request.h"
#include "write_with_pb_replication_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVChunk::TVChunk(
    NActors::TActorSystem* actorSystem,
    IPartitionDirectService* partitionDirectService,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize,
    ui64 vChunkSize,
    TDuration writeHedgingDelay,
    TDuration writeRequestTimeout,
    TDuration traceSamplePeriod,
    NMonitoring::TDynamicCounterPtr counters)
    : ActorSystem(actorSystem)
    , PartitionDirectService(partitionDirectService)
    , Executor(directBlockGroup->GetExecutor())
    , DirectBlockGroup(std::move(directBlockGroup))
    , VChunkConfig(vChunkConfig)
    , BlockSize(DefaultBlockSize)
    , BlocksCount(vChunkSize / BlockSize)
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , WriteHedgingDelay(writeHedgingDelay)
    , WriteRequestTimeout(writeRequestTimeout)
    , TraceSamplePeriod(traceSamplePeriod)
    , Counters(counters)
{
    Y_ABORT_UNLESS(vChunkSize % BlockSize == 0);
}

TVChunk::~TVChunk() = default;

void TVChunk::Start()
{
    // ActorSystem thread

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this()]() mutable
        {
            // Executor thread
            if (auto self = weakSelf.lock()) {
                self->DoStart();
            }
        });
}

TFuture<TReadBlocksLocalResponse> TVChunk::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    const NWilson::TTraceId& traceId)
{
    // VHost thread

    auto span = std::make_shared<NWilson::TSpan>(NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        traceId.Clone(),
        "TVChunk.Read",
        NWilson::EFlags::AUTO_END,
        ActorSystem));
    span->Attribute("VChunkIndex", VChunkConfig.VChunkIndex);

    const TBlockRange64 regionRange = TranslateToRegion(
        *request->Headers.VolumeConfig,
        request->Headers.Range);
    const TBlockRange64 vchunkRange =
        TranslateToVChunk(*request->Headers.VolumeConfig, regionRange);

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "ReadBlocksLocal. Range %s, Region range %s, VChunk range %s",
        request->Headers.Range.Print().c_str(),
        regionRange.Print().c_str(),
        vchunkRange.Print().c_str());

    if (vchunkRange.Start >= BlocksCount) {
        return MakeFuture<TReadBlocksLocalResponse>(TReadBlocksLocalResponse{
            .Error = MakeError(E_ARGUMENT, "out of range")});
    }

    auto promise = TTracedPromise<TReadBlocksLocalResponse>(
        span,
        NKikimr::TWilsonNbs::NbsBasic);
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         vchunkRange,
         callContext = std::move(callContext),
         request = std::move(request),
         span = std::move(span)]() mutable
        {
            // Executor thread
            span->Event("ExecutorTread");

            if (auto self = weakSelf.lock()) {
                self->DoReadBlocksLocal(
                    std::move(promise),
                    vchunkRange,
                    std::move(callContext),
                    std::move(request),
                    std::move(span));
            } else {
                promise.SetValue(
                    TReadBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

TFuture<TWriteBlocksLocalResponse> TVChunk::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    EWriteMode writeMode,
    TDuration pbufferReplyTimeout,
    ui64 lsn,
    const NWilson::TTraceId& traceId)
{
    // VHost thread

    auto span = std::make_shared<NWilson::TSpan>(NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        traceId.Clone(),
        "TVChunk.Write",
        NWilson::EFlags::AUTO_END,
        ActorSystem));
    span->Attribute("VChunkIndex", VChunkConfig.VChunkIndex);

    const TBlockRange64 regionRange = TranslateToRegion(
        *request->Headers.VolumeConfig,
        request->Headers.Range);
    const TBlockRange64 vchunkRange =
        TranslateToVChunk(*request->Headers.VolumeConfig, regionRange);

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "WriteBlocksLocal. Range %s, Region range %s, VChunk range %s",
        request->Headers.Range.Print().c_str(),
        regionRange.Print().c_str(),
        vchunkRange.Print().c_str());

    if (vchunkRange.Start >= BlocksCount) {
        return MakeFuture<TWriteBlocksLocalResponse>(TWriteBlocksLocalResponse{
            .Error = MakeError(E_ARGUMENT, "out of range")});
    }

    auto promise = TTracedPromise<TWriteBlocksLocalResponse>(
        span,
        NKikimr::TWilsonNbs::NbsBasic);
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         vchunkRange,
         callContext = std::move(callContext),
         request = std::move(request),
         writeMode,
         pbufferReplyTimeout,
         lsn,
         span = std::move(span)]() mutable
        {
            // Executor thread
            span->Event("ExecutorTread");

            if (auto self = weakSelf.lock()) {
                self->DoWriteBlocksLocal(
                    std::move(promise),
                    vchunkRange,
                    std::move(callContext),
                    std::move(request),
                    writeMode,
                    pbufferReplyTimeout,
                    lsn,
                    std::move(span));
            } else {
                promise.SetValue(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

////////////////////////////////////////////////////////////////////////////////

void TVChunk::UpdateDirtyMap(const TDBGRestoreResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto pbuffersMap = VChunkConfig.GetPBuffersMap();
    for (const auto& meta: response.Meta) {
        BlocksDirtyMap.RestorePBuffer(
            meta.Lsn,
            meta.Range,
            pbuffersMap[meta.HostIndex]);
    }
    DirtyMapRestored = true;

    DoFlush();
    DoErase();
}

void TVChunk::DoStart()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto future =
        DirectBlockGroup->RestoreDBGPBuffers(VChunkConfig.VChunkIndex);
    future.Subscribe(
        [weakSelf = weak_from_this()]   //
        (const TFuture<TDBGRestoreResponse>& f) mutable
        {
            if (auto self = weakSelf.lock()) {
                self->UpdateDirtyMap(f.GetValue());
            }
        });
}

void TVChunk::DoReadBlocksLocal(
    TTracedPromise<TReadBlocksLocalResponse> promise,
    TBlockRange64 vchunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    std::shared_ptr<NWilson::TSpan> span)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!DirtyMapRestored) {
        auto error = MakeError(E_REJECTED, "dirty map not restored");
        auto ender = TEndSpanWithError(span, error);
        promise.SetValue(TReadBlocksLocalResponse{.Error = std::move(error)});
        return;
    }

    TReadHint readHint;
    {
        auto dirtyMapSpan = span->CreateChild(
            NKikimr::TWilsonNbs::NbsBasic,
            "TVChunk.DirtyMap.ReadHint",
            NWilson::EFlags::AUTO_END);

        readHint = BlocksDirtyMap.MakeReadHint(vchunkRange);
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "Read hint: %s",
            readHint.DebugPrint().c_str());
    }

    if (readHint.RangeHints.empty()) {
        // Will try to repeat the request when the data is ready.
        span->Event("WaitDataReady");

        Executor->ExecuteSimple(
            [weakSelf = weak_from_this(),
             executor = Executor,
             waitReady = readHint.WaitReady,
             promise = std::move(promise),
             vchunkRange,
             callContext = std::move(callContext),
             request = std::move(request),
             span = std::move(span)]() mutable
            {
                executor->WaitFor(waitReady);
                if (auto self = weakSelf.lock()) {
                    span->Event("DataReady");

                    self->DoReadBlocksLocal(
                        std::move(promise),
                        vchunkRange,
                        std::move(callContext),
                        std::move(request),
                        std::move(span));
                } else {
                    promise.SetValue(TReadBlocksLocalResponse{
                        .Error = MakeError(E_CANCELLED)});
                }
            });
        return;
    }

    span->Event("ReadRequestExecutor");
    span->Attribute(
        "SourceCount",
        static_cast<i64>(readHint.RangeHints.size()));

    auto requestExecutor = CreateReadRequestExecutor(
        ActorSystem,
        VChunkConfig,
        DirectBlockGroup,
        std::move(readHint),
        std::move(callContext),
        std::move(request),
        span->GetTraceId());

    auto future = requestExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         span,
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TFuture<TReadRequestResponse>& f) mutable
        {
            Y_ABORT_UNLESS(threadChecker.Check());

            auto value = UnsafeExtractValue(f);

            if (auto self = weakSelf.lock()) {
                bool ok = !HasError(value.Error);
                self->Counters.RequestFinished(EVChunkOperation::Read, ok);
            }

            promise.SetValue(
                TReadBlocksLocalResponse{.Error = std::move(value.Error)});
        });

    span->Event("Run ReadRequestExecutor");
    requestExecutor->Run();
}

void TVChunk::DoWriteBlocksLocal(
    TTracedPromise<TWriteBlocksLocalResponse> promise,
    TBlockRange64 vchunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    EWriteMode writeMode,
    TDuration pbufferReplyTimeout,
    ui64 lsn,
    std::shared_ptr<NWilson::TSpan> span)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    std::shared_ptr<TBaseWriteRequestExecutor> writeExecutor;
    switch (writeMode) {
        case EWriteMode::PBufferReplication:
            writeExecutor =
                std::make_shared<TWriteWithPbReplicationRequestExecutor>(
                    ActorSystem,
                    VChunkConfig,
                    DirectBlockGroup,
                    vchunkRange,
                    std::move(callContext),
                    std::move(request),
                    lsn,
                    span->GetTraceId(),
                    WriteHedgingDelay,
                    WriteRequestTimeout,
                    pbufferReplyTimeout);
            break;
        case EWriteMode::DirectPBuffersFilling:
            writeExecutor =
                std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                    ActorSystem,
                    VChunkConfig,
                    DirectBlockGroup,
                    vchunkRange,
                    std::move(callContext),
                    std::move(request),
                    lsn,
                    span->GetTraceId(),
                    WriteHedgingDelay,
                    WriteRequestTimeout);
            break;
    }

    auto future = writeExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this(),
         vchunkRange,
         promise = std::move(promise),
         span]   //
        (const TFuture<TBaseWriteRequestExecutor::TResponse>& f) mutable
        {
            auto self = weakSelf.lock();
            if (!self) {
                promise.SetValue(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
                return;
            }
            self->OnWriteBlocksResponse(
                std::move(promise),
                vchunkRange,
                f.GetValue(),
                std::move(span));
        });

    span->Event("Run");
    writeExecutor->Run();
}

void TVChunk::OnWriteBlocksResponse(
    TTracedPromise<TWriteBlocksLocalResponse> promise,
    TBlockRange64 range,
    const TBaseWriteRequestExecutor::TResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    {
        auto dirtyMapSpan = span->CreateChild(
            NKikimr::TWilsonNbs::NbsBasic,
            "TVChunk.UpdateDirtyMap",
            NWilson::EFlags::AUTO_END);

        BlocksDirtyMap.WriteFinished(
            response.Lsn,
            range,
            response.RequestedWrites,
            response.CompletedWrites);
    }

    bool ok = !HasError(response.Error);
    Counters.RequestFinished(EVChunkOperation::Write, ok);

    promise.SetValue(TWriteBlocksLocalResponse{.Error = response.Error});

    span->EndOk();

    UpdatePendingCounters();
    DoFlush();
}

void TVChunk::DoFlush()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto flushBatch = BlocksDirtyMap.MakeFlushHint(SyncRequestsBatchSize);

    for (auto& [route, hint]: flushBatch.TakeAllHints()) {
        auto flushExecutor = std::make_shared<TFlushRequestExecutor>(
            ActorSystem,
            VChunkConfig,
            DirectBlockGroup,
            route,
            std::move(hint),
            PartitionDirectService->CreteRootSpan("Flush"));

        auto future = flushExecutor->GetFuture();
        future.Subscribe(
            [weakSelf = weak_from_this()]   //
            (const TFuture<TFlushRequestExecutor::TResponse>& f) mutable
            {
                // Executor thread
                if (auto self = weakSelf.lock()) {
                    self->OnFlushResponse(f.GetValue());
                }
            });

        flushExecutor->Run();
    }
}

void TVChunk::OnFlushResponse(const TFlushRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    BlocksDirtyMap.FlushFinished(
        response.Route,
        response.FlushOk,
        response.FlushFailed);

    for (size_t i = 0; i < response.FlushOk.size(); ++i) {
        Counters.RequestFinished(EVChunkOperation::Flush, true);
    }
    for (size_t i = 0; i < response.FlushFailed.size(); ++i) {
        Counters.RequestFinished(EVChunkOperation::Flush, false);
    }

    UpdatePendingCounters();
    DoErase();
}

void TVChunk::DoErase()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto hints = BlocksDirtyMap.MakeEraseHint(SyncRequestsBatchSize);

    for (auto& [location, hint]: hints.TakeAllHints()) {
        Y_ABORT_UNLESS(IsPBuffer(location));

        auto eraseExecutor = std::make_shared<TEraseRequestExecutor>(
            ActorSystem,
            VChunkConfig,
            DirectBlockGroup,
            location,
            std::move(hint),
            PartitionDirectService->CreteRootSpan("Erase"));

        auto future = eraseExecutor->GetFuture();
        future.Subscribe(
            [weakSelf = weak_from_this()]   //
            (const TFuture<TEraseRequestExecutor::TResponse>& f) mutable
            {
                // Executor thread
                if (auto self = weakSelf.lock()) {
                    self->OnEraseResponse(f.GetValue());
                }
            });

        eraseExecutor->Run();
    }
}

void TVChunk::OnEraseResponse(const TEraseRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    BlocksDirtyMap.EraseFinished(
        response.Location,
        response.EraseOk,
        response.EraseFailed);

    for (size_t i = 0; i < response.EraseOk.size(); ++i) {
        Counters.RequestFinished(EVChunkOperation::Erase, true);
    }
    for (size_t i = 0; i < response.EraseFailed.size(); ++i) {
        Counters.RequestFinished(EVChunkOperation::Erase, false);
    }

    UpdatePendingCounters();
}

void TVChunk::UpdatePendingCounters()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    Counters.UpdatePending(
        EVChunkOperation::Flush,
        BlocksDirtyMap.GetFlushPendingCount());
    Counters.UpdatePending(
        EVChunkOperation::Erase,
        BlocksDirtyMap.GetErasePendingCount());
    Counters.UpdateMinLsn(
        EVChunkOperation::Flush,
        BlocksDirtyMap.GetMinFlushPendingLsn());
    Counters.UpdateMinLsn(
        EVChunkOperation::Erase,
        BlocksDirtyMap.GetMinErasePendingLsn());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
