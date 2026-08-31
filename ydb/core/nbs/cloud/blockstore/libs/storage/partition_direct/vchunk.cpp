#include "vchunk.h"

#include "flush_request.h"
#include "partition_direct_service.h"
#include "read_request_executor.h"
#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/trace_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

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

namespace {

NProto::TError MakeVChunkDestroyedError()
{
    return MakeError(E_REJECTED, "VChunk destroyed");
}

NProto::TError MakeVChunkStoppedError()
{
    return MakeError(E_REJECTED, "VChunk stopped");
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVChunk::TVChunk(
    NActors::TActorSystem* actorSystem,
    ITraceService* traceService,
    IPartitionDirectService* partitionDirectService,
    const TDiskDescription& diskDescription,
    const TVChunkConfig& vChunkConfig,
    const TDirtyMapStateProto& dirtyMapState,
    IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize,
    ui64 vChunkSize)
    : ActorSystem(actorSystem)
    , TraceService(traceService)
    , PartitionDirectService(partitionDirectService)
    , DiskDescription(diskDescription)
    , Executor(directBlockGroup->GetExecutor())
    , DirectBlockGroup(std::move(directBlockGroup))
    , BlockSize(DefaultBlockSize)
    , BlocksCount(vChunkSize / BlockSize)
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , LogTitle{GetCycleCount(), TLogTitle::TVChunk{
        .DiskId = DiskDescription.DiskId,
        .TabletId = DiskDescription.TabletId,
        .Generation = DiskDescription.Generation,
        .DBGIndex = vChunkConfig.GetDBGIndex(),
        .VChunkIndex = vChunkConfig.GetVChunkIndex()
     }}
    , VChunkConfig(vChunkConfig)
    , BlocksDirtyMap(std::make_shared<TBlocksDirtyMap>(VChunkConfig, BlockSize, BlocksCount))
{
    Y_ABORT_UNLESS(vChunkSize % BlockSize == 0);
    // ActorSystem thread

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Create",
        LogTitle.GetWithTime().c_str());

    BlocksDirtyMap->Load(dirtyMapState);
}

TVChunk::~TVChunk()
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Destroy",
        LogTitle.GetWithTime().c_str());
}

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
                promise.SetValue(TReadBlocksLocalResponse{
                    .Error = MakeVChunkDestroyedError()});
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
                bundle->SendFinalReply(TWriteBlocksLocalResponse{
                    .Error = MakeVChunkDestroyedError()});
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
    UpdateConfig(
        std::move(prepare),
        TStringBuilder() << "state of " << PrintHostAndNode(hostIndex)
                         << " updated to " << ToString(state));
}

void TVChunk::UpdateHostCount(size_t newHostCount)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (VChunkConfig.GetHostCount() >= newHostCount) {
        return;
    }

    auto prepare = [weakSelf = weak_from_this(),
                    newHostCount]() -> TVChunkConfig
    {
        if (auto self = weakSelf.lock()) {
            TVChunkConfig cfg = self->VChunkConfig;
            while (cfg.GetHostCount() < newHostCount) {
                cfg.AppendHost();
            }
            return cfg;
        }
        return TVChunkConfig{};
    };
    UpdateConfig(std::move(prepare), "Host count increased");
}

const TVChunkConfig& TVChunk::GetConfig() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return VChunkConfig;
}

TExecutorPtr TVChunk::GetExecutor() const
{
    return Executor;
}

TCountAndSize TVChunk::GetPBuffersUsage(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetPBuffersUsage(hostIndex);
}

TCountAndSize TVChunk::GetAheadBlocks(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetAheadBlocks(hostIndex);
}

TCountAndSize TVChunk::GetBehindBlocks(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetBehindBlocks(hostIndex);
}

std::optional<TPBufferKey> TVChunk::GetSafeBarrierForErase() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!DirtyMapReady.HasValue()) {
        // Not restored yet: this vchunk's records may still exist only in the
        // PBuffers and are not inflight, so an empty dirty map does not mean
        // "no constraint". Report the blocking bound so the tablet-wide
        // cleanup skips its tick until every vchunk finishes restoring.
        return TPBufferKey{};
    }

    return BlocksDirtyMap->GetSafeBarrierForErase();
}

TString TVChunk::DebugPrintDirtyMap()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TStringBuilder sb;
    sb << "\nVChunk" << VChunkConfig.DebugPrint() << "\n";
    sb << "DDiskStates: " << BlocksDirtyMap->DebugPrintDDiskState() << "\n";
    sb << "PBuffers:\n" << BlocksDirtyMap->DebugPrintPBuffers();
    sb << "Inflight(" << Inflight.size() << "):\n" << PrintInflight();
    sb << "PBuffersUsage:\n" << BlocksDirtyMap->DebugPrintPBuffersUsage();
    sb << "DDiskLocks: " << BlocksDirtyMap->DebugPrintLockedDDiskRanges()
       << "\n";
    sb << "CloneQueue: " << BlocksDirtyMap->DebugPrintReadyToClone() << "\n";
    sb << "FlushQueue: " << BlocksDirtyMap->DebugPrintReadyToFlush() << "\n";
    sb << "EraseQueue: " << BlocksDirtyMap->DebugPrintReadyToErase() << "\n";
    sb << "AheadBehind:" << BlocksDirtyMap->DebugPrintAheadBehindBrief()
       << "\n";
    sb << "Ahead:\n" << BlocksDirtyMap->DebugPrintAhead();
    sb << "Behind:\n" << BlocksDirtyMap->DebugPrintBehind();
    sb << "DDiskSyncs: " << BlocksDirtyMap->DebugPrintInflightSync() << "\n";
    return sb;
}

TVChunkSnapshot TVChunk::BuildMonSnapshot()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return {
        .VChunkConfig = VChunkConfig,
        .SafeBarrier = GetSafeBarrierForErase(),
        .DirtyMapDump = DebugPrintDirtyMap(),
    };
}

const TVChunkStats& TVChunk::GetStats() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    return Stats;
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
        FormatError(response.Error).Quote().c_str());

    --InflightWritesCount;

    {
        auto dirtyMapSpan = bundle->GetSpan().CreateChild(
            NKikimr::TWilsonNbs::NbsBasic,
            "TVChunk.UpdateDirtyMap",
            NWilson::EFlags::AUTO_END);

        BlocksDirtyMap->WriteFinished(
            response.PBufferKey,
            bundle->GetVChunkRange(),
            response.RequestedWrites,
            response.CompletedWrites);
    }

    bool ok = !HasError(response.Error);
    Stats.RequestFinished(EVChunkOperation::Write, ok);

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

    BlocksDirtyMap->UpdateBelatedEraseQueue(
        completedWrites,
        bundle->GetPBufferKey());

    DoErase(false, TBlocksDirtyMap::EEraseType::Belated);
    DoPersistDirtyMap();
    ScheduleCleaningUp();
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TBlockRange64> TVChunk::GetFreshRange(THostIndex host) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetFreshRange(host);
}

TReadHint TVChunk::MakeReadHint(TBlockRange64 range)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->MakeReadHint(range);
}

TRangeLock TVChunk::MakeDDiskRangeLock(TBlockRange64 range, THostMask mask)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return TRangeLock(BlocksDirtyMap, range, mask);
}

TSyncHint TVChunk::BeginRangeSync(THostIndex host, TBlockRange64 range)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->BeginRangeSync(host, range);
}

void TVChunk::EndRangeSync(ui64 syncId, bool success)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    BlocksDirtyMap->EndRangeSync(syncId, success);
}

void TVChunk::OnCopyProgress(ui64 totalBytes)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    auto prepare = [weakSelf = weak_from_this()]()
    {
        if (auto self = weakSelf.lock()) {
            auto newConfig = self->VChunkConfig;
            for (const auto& [hostIndex, _]: self->Copiers) {
                const auto freshRange = self->GetFreshRange(hostIndex);
                newConfig.SetWatermark(
                    hostIndex,
                    freshRange ? std::optional<ui64>(
                                     freshRange->Start * self->BlockSize)
                               : std::nullopt);
            }
            return newConfig;
        }
        return TVChunkConfig{};
    };
    UpdateConfig(
        std::move(prepare),
        TStringBuilder() << "copy progress " << totalBytes << " bytes");
}

////////////////////////////////////////////////////////////////////////////////

void TVChunk::UpdateDirtyMap(const TDBGRestoreResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (const auto& meta: response.Meta) {
        BlocksDirtyMap->RestorePBuffer(
            meta.PBufferKey,
            meta.Range,
            meta.HostIndex);
    }
    if (!DirtyMapReady.HasValue()) {
        DirtyMapReady.SetValue();
    }

    DoFlush(false);
    DoErase(false, TBlocksDirtyMap::EEraseType::Standard);
    DoPersistDirtyMap();
}

void TVChunk::DoStart()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

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

    if (Stopped) {
        return;
    }

    Stopped = true;

    if (Copiers.empty()) {
        OnStopped();
        return;
    }

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s DoStop copiers: %zu",
        LogTitle.GetWithTime().c_str(),
        Copiers.size());

    TVector<TFuture<TDDiskDataCopier::EResult>> copierStops;
    for (const auto& [_, copier]: Copiers) {
        copierStops.push_back(copier->Stop());
    }
    Copiers.clear();

    auto a = WaitAll(copierStops);
    a.Subscribe(
        [self = shared_from_this()]   //
        (const auto& f)
        {
            Y_UNUSED(f);
            self->OnStopped();   //
        });
}

void TVChunk::OnStopped()
{
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

    if (Stopped) {
        promise.SetValue(
            TReadBlocksLocalResponse{.Error = MakeVChunkStoppedError()});
        return;
    }

    WaitForDirtyMapReady();

    TReadHint readHint;
    {
        auto dirtyMapSpan = span->CreateChild(
            NKikimr::TWilsonNbs::NbsBasic,
            "TVChunk.DirtyMap.ReadHint",
            NWilson::EFlags::AUTO_END);

        readHint = BlocksDirtyMap->MakeReadHint(vchunkRange);
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
                        .Error = MakeVChunkDestroyedError()});
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
    Inflight.push_back(requestExecutor);

    auto future = requestExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         span]   //
        (const TFuture<IReadRequestExecutor::TResponse>& f) mutable
        {
            auto value = UnsafeExtractValue(f);

            if (auto self = weakSelf.lock()) {
                self->OnReadBlocksResponse(value);
            }

            promise.SetValue(
                TReadBlocksLocalResponse{.Error = std::move(value.Error)});
        });

    span->Event("Run ReadRequestExecutor");
    requestExecutor->Run();
}

void TVChunk::OnReadBlocksResponse(
    const IReadRequestExecutor::TResponse& response)
{
    bool ok = !HasError(response.Error);
    Stats.RequestFinished(EVChunkOperation::Read, ok);
    ScheduleCleaningUp();
}

void TVChunk::DoWriteBlocksLocal(std::shared_ptr<TWriteRequestBundle> bundle)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (Stopped) {
        bundle->SendFinalReply(
            TWriteBlocksLocalResponse{.Error = MakeVChunkStoppedError()});
        return;
    }

    WaitForDirtyMapReady();

    // Mint the record id and register the write as inflight on the same
    // executor thread, so the cleanup watermark covers it from the moment of
    // minting.
    const TPBufferKey pBufferKey{
        .Generation = DirectBlockGroup->GetTabletGeneration(),
        .Lsn = PartitionDirectService->GenerateLsn()};
    bundle->SetPBufferKey(pBufferKey);
    BlocksDirtyMap->RegisterInflightWrite(pBufferKey, bundle->GetVChunkRange());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s DoWriteBlocksLocal: pBufferKey %s %s",
        LogTitle.GetWithTime().c_str(),
        pBufferKey.Print().c_str(),
        bundle->GetVChunkRange().Print().c_str());

    auto writeExecutor = CreateWriteRequestExecutor(
        ActorSystem,
        LogTitle,
        VChunkConfig,
        DirectBlockGroup,
        std::move(bundle));
    Inflight.push_back(writeExecutor);

    ++InflightWritesCount;
    writeExecutor->Run();
}

void TVChunk::DoFlush(bool force)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    if (!BlocksDirtyMap->NeedFlush()) {
        return;
    }

    auto flushBatch =
        BlocksDirtyMap->MakeFlushHint(force ? 1 : SyncRequestsBatchSize);

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
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            route,
            std::move(hint),
            TraceService->CreateRootSpan("Flush"));
        Inflight.push_back(flushExecutor);

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

    BlocksDirtyMap->FlushFinished(
        response.Route,
        response.FlushOk,
        response.FlushFailed);

    for (size_t i = 0; i < response.FlushOk.size(); ++i) {
        Stats.RequestFinished(EVChunkOperation::Flush, true);
    }
    for (size_t i = 0; i < response.FlushFailed.size(); ++i) {
        Stats.RequestFinished(EVChunkOperation::Flush, false);
    }

    UpdatePendingCounters();

    DoErase(false, TBlocksDirtyMap::EEraseType::Standard);
    DoPersistDirtyMap();
    ScheduleCleaningUp();
}

void TVChunk::DoErase(bool force, TBlocksDirtyMap::EEraseType eraseType)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!BlocksDirtyMap->NeedErase()) {
        return;
    }

    TEraseHints hints;
    switch (eraseType) {
        case TBlocksDirtyMap::EEraseType::Standard:
            hints = BlocksDirtyMap->MakeEraseHint(
                force ? 1 : SyncRequestsBatchSize);
            break;
        case TBlocksDirtyMap::EEraseType::Belated:
            hints = BlocksDirtyMap->MakeEraseBelatedHint();
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
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            host,
            std::move(hint),
            TraceService->CreateRootSpan("Erase"));
        Inflight.push_back(eraseExecutor);

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

    BlocksDirtyMap->EraseFinished(
        response.Host,
        response.EraseOk,
        response.EraseFailed);

    for (size_t i = 0; i < response.EraseOk.size(); ++i) {
        Stats.RequestFinished(EVChunkOperation::Erase, true);
    }
    for (size_t i = 0; i < response.EraseFailed.size(); ++i) {
        Stats.RequestFinished(EVChunkOperation::Erase, false);
    }

    UpdatePendingCounters();
    ScheduleCleaningUp();
}

void TVChunk::OnEraseBelatedResponse(
    const TEraseRequestExecutor::TResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (size_t i = 0; i < response.EraseOk.size(); ++i) {
        Stats.RequestFinished(EVChunkOperation::EraseBelated, true);
    }
    for (size_t i = 0; i < response.EraseFailed.size(); ++i) {
        Stats.RequestFinished(EVChunkOperation::EraseBelated, false);
    }

    UpdatePendingCounters();
    ScheduleCleaningUp();
}

void TVChunk::DoPersistDirtyMap()
{
    if (DirtyMapStatePersisting) {
        return;
    }
    if (!BlocksDirtyMap->NeedPersist()) {
        return;
    }
    DirtyMapStatePersisting = true;

    auto state = BlocksDirtyMap->GetStateForPersist();
    const ui32 stateGeneration = state.GetStateGeneration();
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Will persist dirty map. State generation %u",
        LogTitle.GetWithTime().c_str(),
        stateGeneration);

    auto future = PartitionDirectService->UpdateDirtyMapState(
        VChunkConfig.GetVChunkIndex(),
        std::move(state));
    future.Subscribe(
        [weakSelf = weak_from_this(),
         executor = Executor,
         stateGeneration]   //
        (const TPersistResultFuture& f) mutable
        {
            if (f.GetValue() != EPersistResult::Success) {
                return;
            }
            executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf), stateGeneration]()
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnDirtyMapPersisted(stateGeneration);
                    }
                });
        });
}

void TVChunk::OnDirtyMapPersisted(ui32 stateGeneration)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Dirty map persisted. State generation: %u",
        LogTitle.GetWithTime().c_str(),
        stateGeneration);

    DirtyMapStatePersisting = false;
    BlocksDirtyMap->StatePersisted(stateGeneration);
}

void TVChunk::ScheduleCleaningUp()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (CleaningUpScheduled) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s ScheduleCleaningUp: %s %s",
        LogTitle.GetWithTime().c_str(),
        BlocksDirtyMap->NeedFlush() ? "NeedFlush" : "",
        BlocksDirtyMap->NeedErase() ? "NeedErase" : "");

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

    Inflight.erase(
        std::remove_if(
            Inflight.begin(),
            Inflight.end(),
            [](const IRequestExecutorWeakPtr& p) { return p.expired(); }),
        Inflight.end());

    if (InflightFlushesCount || InflightWritesCount) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s CleaningUp: %s %s",
        LogTitle.GetWithTime().c_str(),
        BlocksDirtyMap->NeedFlush() ? "NeedFlush" : "",
        BlocksDirtyMap->NeedErase() ? "NeedErase" : "");

    DoFlush(true);
    DoErase(true, TBlocksDirtyMap::EEraseType::Standard);
    DoPersistDirtyMap();
}

void TVChunk::UpdatePendingCounters()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    Stats.UpdatePending(
        EVChunkOperation::Flush,
        BlocksDirtyMap->GetFlushPendingCount());
    Stats.UpdatePending(
        EVChunkOperation::Erase,
        BlocksDirtyMap->GetErasePendingCount());
    Stats.UpdatePending(
        EVChunkOperation::EraseBelated,
        BlocksDirtyMap->GetEraseBelatedCount());
    Stats.UpdateMinLsn(
        EVChunkOperation::Flush,
        BlocksDirtyMap->GetMinFlushPendingLsn());
    Stats.UpdateMinLsn(
        EVChunkOperation::Erase,
        BlocksDirtyMap->GetMinErasePendingLsn());
}

void TVChunk::UpdateConfig(TPrepareConfigFunc prepareConfig, TString message)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    PendingVChunkConfigs.push_back(TPendingVChunkConfig{
        .PrepareConfig = std::move(prepareConfig),
        .Message = std::move(message)});
    if (PendingVChunkConfigs.size() == 1) {
        PersistNextPendingConfig();
    }
}

void TVChunk::PersistNextPendingConfig()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (PendingVChunkConfigs.empty()) {
        return;
    }

    auto& pending = *PendingVChunkConfigs.begin();

    pending.Config = std::move(pending.PrepareConfig)();

    if (pending.Config.Empty() || pending.Config == VChunkConfig) {
        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Skip update config: %s %s",
            LogTitle.GetWithTime().c_str(),
            pending.Message.Quote().c_str(),
            pending.Config.DebugPrint().c_str());

        PendingVChunkConfigs.pop_front();
        PersistNextPendingConfig();
        return;
    }

    Y_ABORT_UNLESS(
        pending.Config.GetVChunkIndex() == VChunkConfig.GetVChunkIndex());
    Y_ABORT_UNLESS(pending.Config.IsValid());

    auto onPersisted =
        PartitionDirectService->UpdateVChunkConfig(pending.Config);
    onPersisted.Subscribe(
        [weakSelf = weak_from_this(), executor = Executor]   //
        (const TPersistResultFuture& f) mutable
        {
            if (f.GetValue() != EPersistResult::Success) {
                return;
            }
            executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf)]()
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnConfigPersisted();
                    }
                });
        });
}

void TVChunk::OnConfigPersisted()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(!PendingVChunkConfigs.empty());

    auto persisted = std::move(PendingVChunkConfigs.front());
    PendingVChunkConfigs.pop_front();

    ApplyConfig(std::move(persisted.Config), persisted.Message);
    PersistNextPendingConfig();
    DemoteUnavailableHostsIfNeeded();
}

void TVChunk::ApplyConfig(TVChunkConfig newConfig, const TString& message)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Applying new config %s %s -> %s",
        LogTitle.GetWithTime().c_str(),
        message.Quote().c_str(),
        VChunkConfig.DebugPrint().c_str(),
        newConfig.DebugPrint().c_str());

    VChunkConfig = std::move(newConfig);
    BlocksDirtyMap->UpdateConfig(VChunkConfig);

    for (THostIndex hostIndex = 0; hostIndex < VChunkConfig.GetHostCount();
         ++hostIndex)
    {
        const bool needCopier =
            BlocksDirtyMap->GetFreshRange(hostIndex) != std::nullopt &&
            VChunkConfig.GetDisabledHosts().Get(hostIndex) == false;
        const auto* copier = Copiers.FindPtr(hostIndex);

        if (needCopier && !copier) {
            // Add new copier

            LOG_INFO(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s Copier %s started",
                LogTitle.GetWithTime().c_str(),
                PrintHostAndNode(hostIndex).c_str());

            auto newCopier = Copiers[hostIndex] =
                std::make_shared<TDDiskDataCopier>(
                    ActorSystem,
                    TraceService,
                    PartitionDirectService,
                    DiskDescription,
                    VChunkConfig,
                    DirectBlockGroup,
                    this,
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
        if (!needCopier && copier) {
            // Remove unnecessary copier

            LOG_INFO(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s Copier %s stopping",
                LogTitle.GetWithTime().c_str(),
                PrintHostAndNode(hostIndex).c_str());

            (*copier)->Stop().Subscribe(
                [weakSelf = weak_from_this(), hostIndex]   //
                (const TFuture<TDDiskDataCopier::EResult>& f)
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnCopierStopped(hostIndex, f.GetValue());
                    }
                });
            Copiers.erase(hostIndex);

            // Just in case, clear the locks of the deleted copier.
            BlocksDirtyMap->ClearRangeSyncs(hostIndex);
        }
    }
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
            newConfig.DisableHost(hostIndex);
            const TString message = newConfig.PromoteHostIfNeeded();
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

void TVChunk::OnCopierStopped(
    THostIndex hostIndex,
    TDDiskDataCopier::EResult result)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Copier %s stopped %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostAndNode(hostIndex).c_str(),
        ToString(result).c_str());
}

void TVChunk::OnCopyComplete(
    THostIndex hostIndex,
    TDDiskDataCopier::EResult result)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_LOG(
        *ActorSystem,
        result == TDDiskDataCopier::EResult::Ok ? NActors::NLog::PRI_INFO
                                                : NActors::NLog::PRI_WARN,
        NKikimrServices::NBS_PARTITION,
        "%s CopyDDisk %s finished: %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostAndNode(hostIndex).c_str(),
        ToString(result).c_str());

    if (result != TDDiskDataCopier::EResult::Ok) {
        // TODO (drbasic). Decide what to do in case of a copying error.
        Copiers.erase(hostIndex);
        return;
    }

    auto prepare = [weakSelf = weak_from_this(), hostIndex]()
    {
        if (auto self = weakSelf.lock()) {
            auto newConfig = self->VChunkConfig;
            newConfig.SetWatermark(hostIndex, std::nullopt);
            return newConfig;
        }
        return TVChunkConfig{};
    };
    UpdateConfig(
        std::move(prepare),
        TStringBuilder() << PrintHostAndNode(hostIndex) << " copy finished");
}

void TVChunk::DemoteUnavailableHostsIfNeeded()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (GetDDisksForDemote().Empty()) {
        return;
    }

    auto prepare = [weakSelf = weak_from_this()]()
    {
        if (auto self = weakSelf.lock()) {
            auto newConfig = self->VChunkConfig;
            for (auto hostIndex: self->GetDDisksForDemote()) {
                newConfig.DemoteHost(hostIndex);
            }

            return newConfig;
        }
        return TVChunkConfig{};
    };

    UpdateConfig(std::move(prepare), "Demote unavailable hosts");
}

THostMask TVChunk::GetDDisksForDemote() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    auto healthyDDisks = VChunkConfig.GetHealthyDDisks();
    if (healthyDDisks.Count() < QuorumDirectBlockGroupHostCount) {
        return THostMask::MakeEmpty();
    }

    auto ddiskToDemote = VChunkConfig.GetDDisks()
                             .Exclude(VChunkConfig.GetEnabledDDisks())
                             .Exclude(healthyDDisks);
    return ddiskToDemote;
}

void TVChunk::WaitForDirtyMapReady()
{
    if (!DirtyMapReady.HasValue()) {
        const auto dirtyMapReadyFuture = DirtyMapReady.GetFuture();
        Executor->WaitFor(dirtyMapReadyFuture);
    }
}

TString TVChunk::PrintHostAndNode(THostIndex host) const
{
    return PrintHostAndNodeId(host, DirectBlockGroup->GetNodeId(host));
}

TString TVChunk::PrintInflight() const
{
    TStringBuilder result;
    for (const auto& weakExecutor: Inflight) {
        if (auto executor = weakExecutor.lock()) {
            result << "  " << executor->Print() << "\n";
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
