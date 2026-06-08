#include "vchunk.h"

#include "flush_request.h"
#include "range_translate.h"
#include "read_request_executor.h"
#include "write_request.h"

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
    , LogTitle{GetCycleCount(), TLogTitle::TVChunk{.VChunkIndex = vChunkConfig.GetVChunkIndex()}}
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

NThreading::TFuture<void> TVChunk::Stop()
{
    Executor->ExecuteSimple(
        [self = shared_from_this()]() mutable
        {
            // Executor thread
            self->DoStop();
        });
    return StopPromise.GetFuture();
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
    span->Attribute("VChunkIndex", VChunkConfig.GetVChunkIndex());

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
    const NWilson::TTraceId& traceId)
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
        "%s WriteBlocksLocal. Range %s, Region range %s, VChunk range %s",
        LogTitle.GetWithTime().c_str(),
        request->Headers.Range.Print().c_str(),
        regionRange.Print().c_str(),
        vchunkRange.Print().c_str());

    if (vchunkRange.Start >= BlocksCount) {
        return MakeFuture<TWriteBlocksLocalResponse>(TWriteBlocksLocalResponse{
            .Error = MakeError(E_ARGUMENT, "out of range")});
    }

    auto bundle = std::make_shared<TWriteRequestBundle>(
        ActorSystem,
        weak_from_this(),
        std::move(request),
        traceId,
        std::move(callContext),
        vchunkRange);

    bundle->GetSpan().Attribute("VChunkIndex", VChunkConfig.GetVChunkIndex());

    auto future = bundle->GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(), bundle = std::move(bundle)]   //
        () mutable
        {
            // Executor thread
            bundle->GetSpan().Event("ExecutorTread");

            if (auto self = weakSelf.lock()) {
                self->DoWriteBlocksLocal(std::move(bundle));
            } else {
                bundle->SendFinalReply(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

void TVChunk::SetHostState(THostIndex hostIndex, EHostState state)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto prepare = [weakSelf = weak_from_this(), hostIndex, state]()
    {
        if (auto self = weakSelf.lock()) {
            return self->PrepareNewConfig(hostIndex, state);
        }
        return TVChunkConfig{};
    };
    auto apply = [weakSelf = weak_from_this()]()
    {
        if (auto self = weakSelf.lock()) {
            self->ApplyConfig();
        }
    };

    UpdateConfig(std::move(prepare), std::move(apply));
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

std::optional<ui64> TVChunk::GetSafeBarrierForErase() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap.GetSafeBarrierForErase();
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

void TVChunk::OnWriteBlocksResponse(
    std::shared_ptr<TWriteRequestBundle> bundle,
    const TWriteRequestResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnWriteBlocksResponse: %s %s",
        LogTitle.GetWithTime().c_str(),
        bundle->GetVChunkRange().Print().c_str(),
        FormatError(response.Error).c_str());

    --InflightWritesCount;

    {
        auto dirtyMapSpan = bundle->GetSpan().CreateChild(
            NKikimr::TWilsonNbs::NbsBasic,
            "TVChunk.UpdateDirtyMap",
            NWilson::EFlags::AUTO_END);

        BlocksDirtyMap.WriteFinished(
            response.Lsn,
            bundle->GetVChunkRange(),
            response.RequestedWrites,
            response.CompletedWrites);
    }

    bool ok = !HasError(response.Error);
    Counters.RequestFinished(EVChunkOperation::Write, ok);

    bundle->SendFinalReply(TWriteBlocksLocalResponse{.Error = response.Error});

    UpdatePendingCounters();
    DoFlush(false);
    ScheduleCleaningUp();
}

void TVChunk::OnBelatedWriteBlocksResponse(
    std::shared_ptr<TWriteRequestBundle> bundle,
    THostMask completedWrites)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnWriteBlocksNotify. Range %s",
        LogTitle.GetWithTime().c_str(),
        bundle->GetVChunkRange().Print().c_str());

    BlocksDirtyMap.UpdateBelatedEraseQueue(
        completedWrites,
        bundle->GetLsn(),
        bundle->GetVChunkRange());

    DoErase(false, TBlocksDirtyMap::EEraseType::Belated);
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
        DirectBlockGroup->RestoreDBGPBuffers(VChunkConfig.GetVChunkIndex());
    future.Subscribe(
        [weakSelf = weak_from_this()]   //
        (const TFuture<TDBGRestoreResponse>& f) mutable
        {
            if (auto self = weakSelf.lock()) {
                self->UpdateDirtyMap(f.GetValue());
            }
        });
}

void TVChunk::DoStop()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (StopPromise.HasValue()) {
        return;
    }

    for (const auto& [_, copier]: Copiers) {
        copier->Stop();
    }
    Copiers.clear();

    StopPromise.SetValue();
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

void TVChunk::DoWriteBlocksLocal(std::shared_ptr<TWriteRequestBundle> bundle)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    // Generate the lsn and register the write as inflight on the same executor
    // thread, so the cleanup watermark covers it from the moment of generation.
    const ui64 lsn = PartitionDirectService->GenerateLsn();
    bundle->SetLsn(lsn);
    BlocksDirtyMap.RegisterInflightWrite(lsn, bundle->GetVChunkRange());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s DoWriteBlocksLocal: lsn %lu %s",
        LogTitle.GetWithTime().c_str(),
        lsn,
        bundle->GetVChunkRange().Print().c_str());

    auto writeExecutor = CreateWriteRequestExecutor(
        ActorSystem,
        LogTitle,
        VChunkConfig,
        DirectBlockGroup,
        std::move(bundle));

    ++InflightWritesCount;
    writeExecutor->Run();
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

void TVChunk::UpdateConfig(
    TPrepareConfigFunc prepareConfig,
    TApplyPersistedConfigFunc applyPersisted)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    PendingVChunkConfigs.push_back(TPendingVChunkConfig{
        .PrepareConfig = std::move(prepareConfig),
        .ApplyPersisted = std::move(applyPersisted)});
    PersistNextPendingConfig();
}

void TVChunk::PersistNextPendingConfig()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (PendingVChunkConfigs.size() != 1) {
        return;
    }

    auto& pending = *PendingVChunkConfigs.begin();

    pending.Config = std::move(pending.PrepareConfig)();
    Y_ABORT_UNLESS(
        pending.Config.GetVChunkIndex() == VChunkConfig.GetVChunkIndex());
    Y_ABORT_UNLESS(pending.Config.IsValid());

    PartitionDirectService->UpdateVChunkConfig(pending.Config);

    DirectBlockGroup->Schedule(
        TDuration::Seconds(1),
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->OnConfigPersisted();
            }
        });
}

void TVChunk::OnConfigPersisted()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(!PendingVChunkConfigs.empty());

    auto& pending = *PendingVChunkConfigs.begin();
    VChunkConfig = pending.Config;
    auto apply = std::move(pending.ApplyPersisted);
    PendingVChunkConfigs.pop_front();
    PersistNextPendingConfig();
    apply();
}

TVChunkConfig TVChunk::PrepareNewConfig(
    THostIndex hostIndex,
    EHostState state) const
{
    auto newConfig = VChunkConfig;

    switch (state) {
        case EHostState::Online: {
            newConfig.EnableHost(hostIndex);
            break;
        }
        case EHostState::TemporaryOffline: {
            newConfig.DisableHost(hostIndex);
            break;
        }
        case EHostState::Offline: {
            const TString message = newConfig.EvacuateHost(hostIndex);
            if (!message.empty()) {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "%s %s",
                    LogTitle.GetWithTime().c_str(),
                    message.c_str());
            }

            break;
        }
    }
    return newConfig;
}

void TVChunk::ApplyConfig()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    BlocksDirtyMap.UpdateConfig(VChunkConfig);

    // Remove unnecessary copiers
    for (THostIndex hostIndex = 0; hostIndex < VChunkConfig.GetHostCount();
         ++hostIndex)
    {
        const auto watermark = VChunkConfig.GetWatermark(hostIndex);
        const bool needCopier = watermark != std::nullopt;
        const auto* copier = Copiers.FindPtr(hostIndex);
        if (needCopier || !copier) {
            continue;
        }

        (*copier)->Stop();
        Copiers.erase(hostIndex);
    }

    // Add new copiers
    for (THostIndex hostIndex = 0; hostIndex < VChunkConfig.GetHostCount();
         ++hostIndex)
    {
        const auto watermark = VChunkConfig.GetWatermark(hostIndex);
        const bool needCopier = watermark != std::nullopt;
        const auto* copier = Copiers.FindPtr(hostIndex);

        if (!needCopier || copier) {
            continue;
        }

        BlocksDirtyMap.SetReadWatermark(hostIndex, *watermark);
        auto newCopier = Copiers[hostIndex] =
            std::make_shared<TDDiskDataCopier>(
                ActorSystem,
                PartitionDirectService,
                VChunkConfig,
                DirectBlockGroup,
                &BlocksDirtyMap,
                hostIndex);

        newCopier->Start().Subscribe(
            [weakSelf = weak_from_this(), hostIndex]   //
            (const TFuture<TDDiskDataCopier::EResult>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnCopyComplete(hostIndex, f.GetValue());
                }
            });
    }
}

void TVChunk::OnCopyComplete(
    THostIndex hostIndex,
    TDDiskDataCopier::EResult result)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s CopyDDisk %d finished: %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(hostIndex).c_str(),
        ToString(result).c_str());

    auto prepare = [weakSelf = weak_from_this(), hostIndex]()
    {
        if (auto self = weakSelf.lock()) {
            auto newConfig = self->VChunkConfig;
            newConfig.SetWatermark(hostIndex, std::nullopt);
            return newConfig;
        }
        return TVChunkConfig{};
    };
    auto apply = [weakSelf = weak_from_this()]()
    {
        if (auto self = weakSelf.lock()) {
            self->ApplyConfig();
        }
    };

    UpdateConfig(std::move(prepare), std::move(apply));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
