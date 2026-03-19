#include "vchunk.h"

#include "flush_request.h"
#include "read_request.h"
#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVChunk::TVChunk(
    NActors::TActorSystem* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize,
    TDuration traceSamplePeriod)
    : ActorSystem(actorSystem)
    , Executor(directBlockGroup->GetExecutor())
    , DirectBlockGroup(std::move(directBlockGroup))
    , VChunkConfig(vChunkConfig)
    , BlocksCount(VChunkSize / DefaultBlockSize)
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , TraceSamplePeriod(traceSamplePeriod)
{}

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

NThreading::TFuture<TReadBlocksLocalResponse> TVChunk::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // VHost thread

    if (request->Range.Start >= BlocksCount) {
        return MakeFuture<TReadBlocksLocalResponse>(TReadBlocksLocalResponse{
            .Error = MakeError(E_ARGUMENT, "out of range")});
    }

    auto promise = NThreading::NewPromise<TReadBlocksLocalResponse>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         callContext = std::move(callContext),
         request = std::move(request),
         traceId = std::move(traceId)]() mutable
        {
            // Executor thread

            if (auto self = weakSelf.lock()) {
                self->DoReadBlocksLocal(
                    std::move(promise),
                    std::move(callContext),
                    std::move(request),
                    std::move(traceId));
            } else {
                promise.SetValue(
                    TReadBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

NThreading::TFuture<TWriteBlocksLocalResponse> TVChunk::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // VHost thread

    if (request->Range.Start >= BlocksCount) {
        return MakeFuture<TWriteBlocksLocalResponse>(TWriteBlocksLocalResponse{
            .Error = MakeError(E_ARGUMENT, "out of range")});
    }

    auto promise = NThreading::NewPromise<TWriteBlocksLocalResponse>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         callContext = std::move(callContext),
         request = std::move(request),
         traceId = std::move(traceId)]() mutable
        {
            if (auto self = weakSelf.lock()) {
                self->DoWriteBlocksLocal(
                    std::move(promise),
                    std::move(callContext),
                    std::move(request),
                    std::move(traceId));
            } else {
                promise.SetValue(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

////////////////////////////////////////////////////////////////////////////////

NWilson::TTraceId TVChunk::SpanTrace()
{
    return NWilson::TTraceId::NewTraceIdThrottled(
        15,                           // verbosity
        4095,                         // timeToLive
        LastTraceTs,                  // atomic counter for throttling
        NActors::TMonotonic::Now(),   // current monotonic time
        TraceSamplePeriod             // 100ms between samples
    );
}

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
    BlocksDirtyMap.PrepareReadyItems();
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
    TPromise<TReadBlocksLocalResponse> promise,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!DirtyMapRestored) {
        promise.SetValue(TReadBlocksLocalResponse{
            .Error = MakeError(E_REJECTED, "dirty map not restored")});
        return;
    }

    auto hints = BlocksDirtyMap.MakeReadHint(request->Range);
    auto requestExecutor = std::make_shared<TReadRequestExecutor>(
        ActorSystem,
        VChunkConfig,
        DirectBlockGroup,
        std::move(hints),
        std::move(callContext),
        std::move(request),
        std::move(traceId));

    auto future = requestExecutor->GetFuture();
    future.Subscribe(
        [promise = std::move(promise),
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const NThreading::TFuture<TReadRequestExecutor::TResponse>& f) mutable
        {
            Y_ABORT_UNLESS(threadChecker.Check());

            auto value = UnsafeExtractValue(f);
            promise.SetValue(
                TReadBlocksLocalResponse{.Error = std::move(value.Error)});
        });

    requestExecutor->Run();
}

void TVChunk::DoWriteBlocksLocal(
    TPromise<TWriteBlocksLocalResponse> promise,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto range = request->Range;
    auto writeExecutor = std::make_shared<TWriteRequestExecutor>(
        ActorSystem,
        VChunkConfig,
        DirectBlockGroup,
        std::move(callContext),
        std::move(request),
        std::move(traceId));
    auto future = writeExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this(),
         range,
         promise = std::move(promise)]   //
        (const NThreading::TFuture<TWriteRequestExecutor::TResponse>& f) mutable
        {
            // Executor thread
            auto self = weakSelf.lock();
            if (!self) {
                promise.SetValue(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
                return;
            }
            self->OnWriteBlocksResponse(
                std::move(promise),
                range,
                f.GetValue());
        });

    writeExecutor->Run();
}

void TVChunk::OnWriteBlocksResponse(
    NThreading::TPromise<TWriteBlocksLocalResponse> promise,
    TBlockRange64 range,
    const TWriteRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    promise.SetValue(TWriteBlocksLocalResponse{.Error = response.Error});

    BlocksDirtyMap.WriteFinished(
        response.Lsn,
        range,
        response.RequestedWrites,
        response.CompletedWrites);

    DoFlush();
}

void TVChunk::DoFlush()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto hints = BlocksDirtyMap.MakeFlushHint(SyncRequestsBatchSize);

    for (auto& [location, hint]: hints) {
        Y_ABORT_UNLESS(IsPBuffer(location));

        auto flushExecutor = std::make_shared<TFlushRequestExecutor>(
            ActorSystem,
            VChunkConfig,
            DirectBlockGroup,
            location,
            std::move(hint),
            SpanTrace());

        auto future = flushExecutor->GetFuture();
        future.Subscribe(
            [weakSelf = weak_from_this()]   //
            (const NThreading::TFuture<TFlushRequestExecutor::TResponse>&
                 f) mutable
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
        response.Location,
        response.FlushOk,
        response.FlushFailed);

    DoErase();
}

void TVChunk::DoErase()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto hints = BlocksDirtyMap.MakeEraseHint(SyncRequestsBatchSize);

    for (auto& [location, hint]: hints) {
        Y_ABORT_UNLESS(IsPBuffer(location));

        auto eraseExecutor = std::make_shared<TEraseRequestExecutor>(
            ActorSystem,
            VChunkConfig,
            DirectBlockGroup,
            location,
            std::move(hint),
            SpanTrace());

        auto future = eraseExecutor->GetFuture();
        future.Subscribe(
            [weakSelf = weak_from_this()]   //
            (const NThreading::TFuture<TEraseRequestExecutor::TResponse>&
                 f) mutable
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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
