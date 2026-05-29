#include "vchunk.h"

#include "flush_request.h"
#include "range_translate.h"
#include "read_request_executor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TVChunk::TVChunk(
    NActors::TActorSystem* actorSystem,
    IPartitionDirectService* partitionDirectService,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize,
    ui64 vChunkSize,
    NMonitoring::TDynamicCounterPtr counters)
    : ActorSystem(actorSystem)
    , PartitionDirectService(partitionDirectService)
    , Executor(directBlockGroup->GetExecutor())
    , DirectBlockGroup(std::move(directBlockGroup))
    , BlockSize(DefaultBlockSize)
    , BlocksCount(vChunkSize / BlockSize)
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , LogTitle{GetCycleCount(), TLogTitle::TVChunk{.VChunkIndex = vChunkConfig.VChunkIndex}}
    , VChunkConfig(vChunkConfig)
    , BlocksDirtyMap(VChunkConfig, BlockSize, BlocksCount)
    , Counters(std::move(counters))
{
    Y_ABORT_UNLESS(vChunkSize % BlockSize == 0);
    // ActorSystem thread
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
        "%s ReadBlocksLocal. Range %s, Region range %s, VChunk range %s",
        LogTitle.GetWithTime().c_str(),
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
        "%s WriteBlocksLocal. Range %s, Region range %s, VChunk range %s",
        LogTitle.GetWithTime().c_str(),
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
                    lsn,
                    std::move(span));
            } else {
                promise.SetValue(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

void TVChunk::SetHostState(THostIndex hostIndex, EHostState state)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    switch (state) {
        case EHostState::Enabled: {
            VChunkConfig.EnableHost(hostIndex);
            break;
        }
        case EHostState::Disabled: {
            VChunkConfig.DisableHost(hostIndex);
            break;
        }
    }

    BlocksDirtyMap.UpdateConfig(
        VChunkConfig.GetDesiredDDisks(),
        VChunkConfig.GetDesiredPBuffers(),
        VChunkConfig.GetDisabledHosts());
}

const TVChunkConfig& TVChunk::GetConfig() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return VChunkConfig;
}

ui64 TVChunk::GetPBufferUsedSize(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap.GetPBufferCounters(hostIndex).CurrentBytesCount;
}

TString TVChunk::DebugPrintDirtyMap()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TStringBuilder sb;
    sb << "\nVChunk" << VChunkConfig.DebugPrint() << "\n";
    sb << "DDiskStates: " << BlocksDirtyMap.DebugPrintDDiskState() << "\n";
    sb << "PBuffers:\n" << BlocksDirtyMap.DebugPrintPBuffers();
    sb << "PBuffersUsage:\n" << BlocksDirtyMap.DebugPrintPBuffersUsage();
    sb << "DDiskLocks: " << BlocksDirtyMap.DebugPrintLockedDDiskRanges()
       << "\n";
    sb << "CloneQueue: " << BlocksDirtyMap.DebugPrintReadyToClone() << "\n";
    sb << "FlushQueue: " << BlocksDirtyMap.DebugPrintReadyToFlush() << "\n";
    sb << "EraseQueue: " << BlocksDirtyMap.DebugPrintReadyToErase() << "\n";
    return sb;
}

void TVChunk::UpdateConfig(const TVChunkConfig& newConfig)
{
    Y_ABORT_UNLESS(newConfig.VChunkIndex == VChunkConfig.VChunkIndex);
    Y_ABORT_UNLESS(newConfig.IsValid());
    PartitionDirectService->UpdateVChunkConfig(newConfig);
}

////////////////////////////////////////////////////////////////////////////////

void TVChunk::UpdateDirtyMap(const TDBGRestoreResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (const auto& meta: response.Meta) {
        BlocksDirtyMap.RestorePBuffer(meta.Lsn, meta.Range, meta.HostIndex);
    }
    DirtyMapRestored = true;

    DoFlush(false);
    DoErase(false, TBlocksDirtyMap::EEraseType::Standard);
}

void TVChunk::DoStart()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LogTitle.SetDiskId(PartitionDirectService->GetVolumeConfig()->DiskId);
    DirectBlockGroup->Register(weak_from_this());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s DoStart",
        LogTitle.GetWithTime().c_str());

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
            "%s Read hint: %s",
            LogTitle.GetWithTime().c_str(),
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
        LogTitle,
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
        (const TFuture<IReadRequestExecutor::TResponse>& f) mutable
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
    ui64 lsn,
    std::shared_ptr<NWilson::TSpan> span)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s DoWriteBlocksLocal: %s",
        LogTitle.GetWithTime().c_str(),
        vchunkRange.Print().c_str());

    auto writeExecutor = CreateWriteRequestExecutor(
        ActorSystem,
        LogTitle,
        VChunkConfig,
        DirectBlockGroup,
        vchunkRange,
        std::move(callContext),
        std::move(request),
        lsn,
        span->GetTraceId());

    writeExecutor->SetReplyCallback(
        [weakSelf = weak_from_this(),
         vchunkRange,
         promise = std::move(promise),
         span](TBaseWriteRequestExecutor::TResponse response) mutable
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
                response,
                std::move(span));
        });

    writeExecutor->SetNotifyBelatedCallback(
        [weakSelf = weak_from_this(),
         vchunkRange](THostMask completedWrites, ui64 lsn) mutable
        {
            auto self = weakSelf.lock();
            if (self) {
                self->OnWriteBlocksNotifyBelated(
                    vchunkRange,
                    completedWrites,
                    lsn);
            }
        });

    span->Event("Run");
    ++InflightWritesCount;
    writeExecutor->Run();
}

void TVChunk::OnWriteBlocksResponse(
    TTracedPromise<TWriteBlocksLocalResponse> promise,
    TBlockRange64 vchunkRange,
    const TBaseWriteRequestExecutor::TResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnWriteBlocksResponse: %s %s",
        LogTitle.GetWithTime().c_str(),
        vchunkRange.Print().c_str(),
        FormatError(response.Error).c_str());

    --InflightWritesCount;

    {
        auto dirtyMapSpan = span->CreateChild(
            NKikimr::TWilsonNbs::NbsBasic,
            "TVChunk.UpdateDirtyMap",
            NWilson::EFlags::AUTO_END);

        BlocksDirtyMap.WriteFinished(
            response.Lsn,
            vchunkRange,
            response.RequestedWrites,
            response.CompletedWrites);
    }

    bool ok = !HasError(response.Error);
    Counters.RequestFinished(EVChunkOperation::Write, ok);

    promise.SetValue(TWriteBlocksLocalResponse{.Error = response.Error});

    span->EndOk();

    UpdatePendingCounters();
    DoFlush(false);
    ScheduleCleaningUp();
}

void TVChunk::OnWriteBlocksNotifyBelated(
    TBlockRange64 range,
    THostMask completedWrites,
    ui64 lsn)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteBlocksNotify. Range %s",
        range.Print().c_str());

    BlocksDirtyMap.UpdateBelatedEraseQueue(completedWrites, lsn, range);

    DoErase(false, TBlocksDirtyMap::EEraseType::Belated);
}

void TVChunk::DoFlush(bool force)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    if (!BlocksDirtyMap.NeedFlush()) {
        return;
    }

    auto flushBatch =
        BlocksDirtyMap.MakeFlushHint(force ? 1 : SyncRequestsBatchSize);

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s DoFlush: %lu %s",
        LogTitle.GetWithTime().c_str(),
        flushBatch.GetAllHints().size(),
        force ? "force" : "normal");

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

        ++InflightFlushesCount;
        flushExecutor->Run();
    }
}

void TVChunk::OnFlushResponse(const TFlushRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnFlushResponse",
        LogTitle.GetWithTime().c_str());

    --InflightFlushesCount;

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

    DoErase(false, TBlocksDirtyMap::EEraseType::Standard);
    ScheduleCleaningUp();
}

void TVChunk::DoErase(bool force, TBlocksDirtyMap::EEraseType eraseType)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!BlocksDirtyMap.NeedErase()) {
        return;
    }

    TEraseHints hints;
    switch (eraseType) {
        case TBlocksDirtyMap::EEraseType::Standard:
            hints =
                BlocksDirtyMap.MakeEraseHint(force ? 1 : SyncRequestsBatchSize);
            break;
        case TBlocksDirtyMap::EEraseType::Belated:
            hints = BlocksDirtyMap.MakeEraseBelatedHint();
            break;
    };

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s DoErase: %lu %s",
        LogTitle.GetWithTime().c_str(),
        hints.GetAllHints().size(),
        force ? "force" : "normal");

    for (auto& [host, hint]: hints.TakeAllHints()) {
        auto eraseExecutor = std::make_shared<TEraseRequestExecutor>(
            ActorSystem,
            VChunkConfig,
            DirectBlockGroup,
            host,
            std::move(hint),
            PartitionDirectService->CreteRootSpan("Erase"));

        auto future = eraseExecutor->GetFuture();
        future.Subscribe(
            [weakSelf = weak_from_this(), eraseType]   //
            (const TFuture<TEraseRequestExecutor::TResponse>& f) mutable
            {
                // Executor thread
                if (auto self = weakSelf.lock()) {
                    switch (eraseType) {
                        case TBlocksDirtyMap::EEraseType::Standard:
                            self->OnEraseResponse(f.GetValue());
                            break;
                        case TBlocksDirtyMap::EEraseType::Belated:
                            self->OnEraseBelatedResponse(f.GetValue());
                            break;
                    };
                }
            });

        eraseExecutor->Run();
    }
}

void TVChunk::OnEraseResponse(const TEraseRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnEraseResponse",
        LogTitle.GetWithTime().c_str());

    BlocksDirtyMap.EraseFinished(
        response.Host,
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

void TVChunk::OnEraseBelatedResponse(
    const TEraseRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (size_t i = 0; i < response.EraseOk.size(); ++i) {
        Counters.RequestFinished(EVChunkOperation::EraseBelated, true);
    }
    for (size_t i = 0; i < response.EraseFailed.size(); ++i) {
        Counters.RequestFinished(EVChunkOperation::EraseBelated, false);
    }

    UpdatePendingCounters();
}

void TVChunk::ScheduleCleaningUp()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (CleaningUpScheduled || InflightFlushesCount || InflightWritesCount) {
        return;
    }

    if (!BlocksDirtyMap.NeedFlush() && !BlocksDirtyMap.NeedErase()) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s ScheduleCleaningUp: %s %s",
        LogTitle.GetWithTime().c_str(),
        BlocksDirtyMap.NeedFlush() ? "NeedFlush" : "",
        BlocksDirtyMap.NeedErase() ? "NeedErase" : "");

    CleaningUpScheduled = true;

    DirectBlockGroup->Schedule(
        TDuration::Seconds(1),
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->CleaningUpScheduled = false;
                self->CleaningUp();
            }
        });
}

void TVChunk::CleaningUp()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (InflightFlushesCount || InflightWritesCount) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s CleaningUp: %s %s",
        LogTitle.GetWithTime().c_str(),
        BlocksDirtyMap.NeedFlush() ? "NeedFlush" : "",
        BlocksDirtyMap.NeedErase() ? "NeedErase" : "");

    DoFlush(true);
    DoErase(true, TBlocksDirtyMap::EEraseType::Standard);
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
    Counters.UpdatePending(
        EVChunkOperation::EraseBelated,
        BlocksDirtyMap.GetEraseBelatedCount());
    Counters.UpdateMinLsn(
        EVChunkOperation::Flush,
        BlocksDirtyMap.GetMinFlushPendingLsn());
    Counters.UpdateMinLsn(
        EVChunkOperation::Erase,
        BlocksDirtyMap.GetMinErasePendingLsn());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
