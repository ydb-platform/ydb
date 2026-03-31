#include "vchunk.h"

#include "flush_request.h"
#include "range_translate.h"
#include "read_request.h"
#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>

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
    IPartitionDirectService* partitionDirectService,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize,
    TDuration traceSamplePeriod)
    : ActorSystem(actorSystem)
    , PartitionDirectService(partitionDirectService)
    , Executor(directBlockGroup->GetExecutor())
    , DirectBlockGroup(std::move(directBlockGroup))
    , VChunkConfig(vChunkConfig)
    , BlocksCount(VChunkSize / DefaultBlockSize)
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , TraceSamplePeriod(traceSamplePeriod)
{
    Y_UNUSED(PartitionDirectService);
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
    NWilson::TTraceId traceId)
{
    // VHost thread

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

    auto promise = NewPromise<TReadBlocksLocalResponse>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         vchunkRange,
         callContext = std::move(callContext),
         request = std::move(request),
         traceId = std::move(traceId)]() mutable
        {
            // Executor thread

            if (auto self = weakSelf.lock()) {
                self->DoReadBlocksLocal(
                    std::move(promise),
                    vchunkRange,
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

TFuture<TWriteBlocksLocalResponse> TVChunk::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    EWriteMode writeMode,
    ui32 pbufferReplyTimeoutMicroseconds,
    NWilson::TTraceId traceId)
{
    // VHost thread

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

    auto promise = NewPromise<TWriteBlocksLocalResponse>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         vchunkRange,
         callContext = std::move(callContext),
         request = std::move(request),
         writeMode,
         pbufferReplyTimeoutMicroseconds,
         traceId = std::move(traceId)]() mutable
        {
            if (auto self = weakSelf.lock()) {
                self->DoWriteBlocksLocal(
                    std::move(promise),
                    vchunkRange,
                    std::move(callContext),
                    std::move(request),
                    writeMode,
                    pbufferReplyTimeoutMicroseconds,
                    std::move(traceId));
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
    TPromise<TReadBlocksLocalResponse> promise,
    TBlockRange64 vchunkRange,
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

    auto readHint = BlocksDirtyMap.MakeReadHint(vchunkRange);

    if (readHint.RangeHints.empty()) {
        // Will try to repeat the request when the data is ready.
        Executor->ExecuteSimple(
            [weakSelf = weak_from_this(),
             executor = Executor,
             waitReady = readHint.WaitReady,
             promise = std::move(promise),
             vchunkRange,
             callContext = std::move(callContext),
             request = std::move(request),
             traceId = std::move(traceId)]() mutable
            {
                executor->WaitFor(waitReady);
                if (auto self = weakSelf.lock()) {
                    self->DoReadBlocksLocal(
                        std::move(promise),
                        vchunkRange,
                        std::move(callContext),
                        std::move(request),
                        std::move(traceId));
                } else {
                    promise.SetValue(TReadBlocksLocalResponse{
                        .Error = MakeError(E_CANCELLED)});
                }
            });
        return;
    }

    auto requestExecutor = std::make_shared<TReadRequestExecutor>(
        ActorSystem,
        VChunkConfig,
        DirectBlockGroup,
        std::move(readHint),
        std::move(callContext),
        std::move(request),
        std::move(traceId));

    auto future = requestExecutor->GetFuture();
    future.Subscribe(
        [promise = std::move(promise),
         threadChecker = ExecutorThreadChecker.CreateDelegate()]   //
        (const TFuture<TReadRequestExecutor::TResponse>& f) mutable
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
    TBlockRange64 vchunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    EWriteMode writeMode,
    ui32 pbufferReplyTimeoutMicroseconds,
    NWilson::TTraceId traceId)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto writeExecutor = std::make_shared<TWriteRequestExecutor>(
        ActorSystem,
        VChunkConfig,
        DirectBlockGroup,
        vchunkRange,
        std::move(callContext),
        std::move(request),
        std::move(traceId));
    auto future = writeExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this(),
         vchunkRange,
         promise = std::move(promise)]   //
        (const TFuture<TWriteRequestExecutor::TResponse>& f) mutable
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
                vchunkRange,
                f.GetValue());
        });

    writeExecutor->Run(writeMode, pbufferReplyTimeoutMicroseconds);
}

void TVChunk::OnWriteBlocksResponse(
    TPromise<TWriteBlocksLocalResponse> promise,
    TBlockRange64 range,
    const TWriteRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    BlocksDirtyMap.WriteFinished(
        response.Lsn,
        range,
        response.RequestedWrites,
        response.CompletedWrites);

    promise.SetValue(TWriteBlocksLocalResponse{.Error = response.Error});

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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
