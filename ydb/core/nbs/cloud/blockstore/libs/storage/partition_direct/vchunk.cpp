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

<<<<<<< HEAD
=======
namespace {

ui32 CheckedVChunkBlockSize(ui32 blockSize)
{
    Y_ABORT_UNLESS(IsSupportedBlockSize(blockSize));
    return blockSize;
}

NProto::TError MakeVChunkDestroyedError()
{
    return MakeError(E_REJECTED, "VChunk destroyed");
}

NProto::TError MakeVChunkStoppedError()
{
    return MakeError(E_REJECTED, "VChunk stopped");
}

// Finds DDisks that have Behind in the proto.
THostMask GetFreshDDisks(const TDirtyMapStateProto& dirtyMapState)
{
    THostMask result;
    THostIndex host = 0;
    for (const auto& ddiskState: dirtyMapState.GetDDiskStates()) {
        const auto& behind = ddiskState.GetBehind();
        if (behind.GetEncodingCase() != TBlockFieldProto::ENCODING_NOT_SET) {
            result.Set(host);
        }
        ++host;
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
TVChunk::TVChunk(
    NActors::TActorSystem* actorSystem,
    IPartitionDirectService* partitionDirectService,
    const TVChunkConfig& vChunkConfig,
<<<<<<< HEAD
=======
    bool touched,
    const TDirtyMapStateProto& dirtyMapState,
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
    IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize,
    ui64 vChunkSize,
    NMonitoring::TDynamicCounterPtr counters)
    : ActorSystem(actorSystem)
    , PartitionDirectService(partitionDirectService)
    , Executor(directBlockGroup->GetExecutor())
    , DirectBlockGroup(std::move(directBlockGroup))
<<<<<<< HEAD
    , BlockSize(DefaultBlockSize)
    , BlocksCount(vChunkSize / BlockSize)
=======
    , BlockSize(CheckedVChunkBlockSize(blockSize))
    , BlocksCount(GetVChunkBlockCount(BlockSize, vChunkSize))
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , LogTitle{GetCycleCount(), TLogTitle::TVChunk{
        .DBGIndex = vChunkConfig.GetDBGIndex(),
        .VChunkIndex = vChunkConfig.GetVChunkIndex()
     }}
    , VChunkConfig(vChunkConfig)
<<<<<<< HEAD
<<<<<<< HEAD
    , BlocksDirtyMap(VChunkConfig, BlockSize, BlocksCount)
    , Counters(std::move(counters))
=======
=======
    , TouchedState(
          touched ? ETouchedState::Persisted : ETouchedState::NotTouched)
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
    , BlocksDirtyMap(std::make_shared<TBlocksDirtyMap>(
          DirectBlockGroup->GetArenaAllocatorPool(),
          VChunkConfig,
          IsTouched(),
          dirtyMapState,
          BlockSize,
          BlocksCount))
<<<<<<< HEAD
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
    , PersistedFreshDDisks(BlocksDirtyMap->GetOutdatedDDisks())
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
{
    Y_ABORT_UNLESS(vChunkSize % BlockSize == 0);
    // ActorSystem thread
<<<<<<< HEAD
=======

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Create",
        LogTitle.GetWithTime().c_str());
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
}

TVChunk::~TVChunk() = default;

void TVChunk::Start()
{
    // ActorSystem thread

    LogTitle.SetDiskId(PartitionDirectService->GetVolumeConfig()->DiskId);

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
    const TBlockRange16 vchunkRange =
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
    const TBlockRange16 vchunkRange =
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

void TVChunk::OnHostAppended(size_t newHostCount)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    // Resize synchronously - the config apply below is async, but the new host
    // is connected and used before it runs.
    BlocksDirtyMap.ResizeHosts(newHostCount);

    auto prepare = [weakSelf = weak_from_this()]() -> TVChunkConfig
    {
        if (auto self = weakSelf.lock()) {
            TVChunkConfig cfg = self->VChunkConfig;
            cfg.AppendHost();
            return cfg;
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

<<<<<<< HEAD
ui64 TVChunk::GetPBufferUsedSize(THostIndex hostIndex) const
=======
THostMask TVChunk::GetHealthyDDisks() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return VChunkConfig.GetEnabledDDisks()
        .Exclude(PersistedFreshDDisks)
        .Exclude(BlocksDirtyMap->GetOutdatedDDisks());
}

TExecutorPtr TVChunk::GetExecutor() const
{
    return Executor;
}

TCountAndSize TVChunk::GetPBuffersUsage(THostIndex hostIndex) const
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap.GetPBufferCounters(hostIndex).CurrentBytesCount;
}

<<<<<<< HEAD
<<<<<<< HEAD
std::optional<ui64> TVChunk::GetSafeBarrierForErase() const
=======
ui64 TVChunk::GetFreshTotalBytes(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetFreshTotalBytes(hostIndex);
}

ui64 TVChunk::GetRottenTotalBytes(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetRottenTotalBytes(hostIndex);
}

size_t TVChunk::GetAllocatedMemorySize() const
{
    return BlocksDirtyMap->GetAllocatedSize();
}

size_t TVChunk::GetUsedMemorySize() const
{
    return BlocksDirtyMap->GetUsedSize();
}

=======
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
std::optional<TPBufferKey> TVChunk::GetSafeBarrierForErase() const
>>>>>>> 78a20188184 (Field map set optimization (#52565))
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (!DirtyMapReady.HasValue()) {
        // Not restored yet: this vchunk's records may still exist only in the
        // PBuffers and are not inflight, so an empty dirty map does not mean
        // "no constraint". Report the blocking bound so the tablet-wide
        // cleanup skips its tick until every vchunk finishes restoring.
        return 0;
    }

    return BlocksDirtyMap.GetSafeBarrierForErase();
}

TString TVChunk::DebugPrintDirtyMap()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    TStringBuilder sb;
    sb << "\nVChunk" << VChunkConfig.DebugPrint() << "\n";
    sb << "DDiskStates: " << BlocksDirtyMap.DebugPrintDDiskState() << "\n";
    sb << "PBuffers:\n" << BlocksDirtyMap.DebugPrintPBuffers();
    sb << "Inflight(" << Inflight.size() << "):\n" << PrintInflight();
    sb << "PBuffersUsage:\n" << BlocksDirtyMap.DebugPrintPBuffersUsage();
    sb << "DDiskLocks: " << BlocksDirtyMap.DebugPrintLockedDDiskRanges()
       << "\n";
<<<<<<< HEAD
    sb << "CloneQueue: " << BlocksDirtyMap.DebugPrintReadyToClone() << "\n";
    sb << "FlushQueue: " << BlocksDirtyMap.DebugPrintReadyToFlush() << "\n";
    sb << "EraseQueue: " << BlocksDirtyMap.DebugPrintReadyToErase() << "\n";
=======
    sb << "CloneQueue: " << BlocksDirtyMap->DebugPrintReadyToClone() << "\n";
    sb << "FlushQueue: " << BlocksDirtyMap->DebugPrintReadyToFlush() << "\n";
    sb << "EraseQueue: " << BlocksDirtyMap->DebugPrintReadyToErase() << "\n";
    sb << "BehindBrief:" << BlocksDirtyMap->DebugPrintBehindBrief() << "\n";
    sb << "Behind:\n" << BlocksDirtyMap->DebugPrintBehind();
    sb << "DDiskSyncs: " << BlocksDirtyMap->DebugPrintInflightSync() << "\n";
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    return sb;
}

<<<<<<< HEAD
=======
TDirtyMapStats TVChunk::GetDirtyMapStats() const
{
    return BlocksDirtyMap->GetStats();
}

TDirtyMapHostStats TVChunk::GetDirtyMapHostStats(THostIndex hostIndex) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetHostStats(hostIndex);
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

>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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
    PartitionDirectService->OnWriteFinished();

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

    BlocksDirtyMap.UpdateBelatedEraseQueue(completedWrites, bundle->GetLsn());

    DoErase(false, TBlocksDirtyMap::EEraseType::Belated);
<<<<<<< HEAD
=======
    StartPersist();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    ScheduleCleaningUp();
}

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD
=======
std::optional<TBlockRange16> TVChunk::GetFreshRange(THostIndex host) const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->GetFreshRange(host);
}

TReadHint TVChunk::MakeReadHint(TBlockRange16 range)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return BlocksDirtyMap->MakeReadHint(range);
}

TRangeLock TVChunk::MakeDDiskRangeLock(TBlockRange16 range, THostMask mask)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    return TRangeLock(BlocksDirtyMap, range, mask);
}

TSyncHint TVChunk::BeginRangeSync(THostIndex host, TBlockRange16 range)
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

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Copy progress %lu bytes",
        LogTitle.GetWithTime().c_str(),
        totalBytes);

    StartPersist();
}

////////////////////////////////////////////////////////////////////////////////

>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
void TVChunk::UpdateDirtyMap(const TDBGRestoreResponse& response)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    for (const auto& meta: response.Meta) {
        BlocksDirtyMap.RestorePBuffer(meta.Lsn, meta.Range, meta.HostIndex);
    }
    if (!DirtyMapReady.HasValue()) {
        DirtyMapReady.SetValue();
    }

    DoFlush(false);
    DoErase(false, TBlocksDirtyMap::EEraseType::Standard);
<<<<<<< HEAD
=======
    StartPersist();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
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

    if (StopPromise.HasValue()) {
        return;
    }

    Stopped = true;

    for (const auto& [_, copier]: Copiers) {
        copier->Stop();
    }
    Copiers.clear();

    StopPromise.SetValue();
}

void TVChunk::DoReadBlocksLocal(
    TTracedPromise<TReadBlocksLocalResponse> promise,
    TBlockRange16 vchunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    std::shared_ptr<NWilson::TSpan> span)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (Stopped) {
        promise.SetValue(TReadBlocksLocalResponse{
            .Error = MakeError(E_CANCELLED, "VChunk is stopped")});
        return;
    }

    WaitForDirtyMapReady();

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
    Inflight.push_back(requestExecutor);

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
    Counters.RequestFinished(EVChunkOperation::Read, ok);
    ScheduleCleaningUp();
}

void TVChunk::DoWriteBlocksLocal(std::shared_ptr<TWriteRequestBundle> bundle)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (Stopped) {
        bundle->SendFinalReply(TWriteBlocksLocalResponse{
            .Error = MakeError(E_CANCELLED, "VChunk is stopped")});
        return;
    }

    WaitForDirtyMapReady();

<<<<<<< HEAD
    // Generate the lsn and register the write as inflight on the same executor
    // thread, so the cleanup watermark covers it from the moment of generation.
    const ui64 lsn = PartitionDirectService->GenerateLsn();
    bundle->SetLsn(lsn);
    BlocksDirtyMap.RegisterInflightWrite(lsn, bundle->GetVChunkRange());
=======
    // Mint the record id and register the write as inflight on the same
    // executor thread, so the cleanup watermark covers it from the moment of
    // minting.
    const auto lsn = PartitionDirectService->OnWriteStarted();
    const TPBufferKey pBufferKey{
        .Generation = DirectBlockGroup->GetTabletGeneration(),
        .Lsn = lsn};
    bundle->SetPBufferKey(pBufferKey);
    BlocksDirtyMap->RegisterInflightWrite(pBufferKey, bundle->GetVChunkRange());
>>>>>>> 67e4af6a66a ([NBS-6826] Dynamic selection of write mode (#53486))

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
    Inflight.push_back(writeExecutor);

    ++InflightWritesCount;
    writeExecutor->Run();
}

void TVChunk::DoFlush(bool force)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    if (!BlocksDirtyMap.NeedFlush()) {
        return;
    }

    Touch();
    if (TouchedState != ETouchedState::Persisted) {
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
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            route,
            std::move(hint),
            PartitionDirectService->CreteRootSpan("Flush"));
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
<<<<<<< HEAD
=======
    StartPersist();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
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
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            host,
            std::move(hint),
            PartitionDirectService->CreteRootSpan("Erase"));
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
    ScheduleCleaningUp();
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
    ScheduleCleaningUp();
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
void TVChunk::DoPersistDirtyMap()
=======
void TVChunk::StartPersist()
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (Persisting) {
        return;
    }

    if (!PendingVChunkConfigs.empty()) {
        PersistNextPendingConfig();
        return;
    }

    DoPersistDirtyMap();
}

void TVChunk::DoPersistDirtyMap()
{
    Y_ABORT_UNLESS(!Persisting);

    if (!BlocksDirtyMap->NeedPersist()) {
        return;
    }
    Persisting = true;

    auto state = BlocksDirtyMap->GetStateForPersist();
    const ui32 stateGeneration = BlocksDirtyMap->GetCurrentGeneration();
    const THostMask freshDDisks = BlocksDirtyMap->GetOutdatedDDisks();
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
         stateGeneration,
         freshDDisks]   //
        (const TPersistResultFuture& f) mutable
        {
            if (f.GetValue() != EPersistResult::Success) {
                return;
            }
            executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf), stateGeneration, freshDDisks]()
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnDirtyMapPersisted(stateGeneration, freshDDisks);
                    }
                });
        });
}

void TVChunk::OnDirtyMapPersisted(ui32 stateGeneration, THostMask freshDDisks)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Dirty map persisted. State generation: %u",
        LogTitle.GetWithTime().c_str(),
        stateGeneration);

    Y_ABORT_UNLESS(Persisting);
    Persisting = false;
    BlocksDirtyMap->StatePersisted(stateGeneration);
    PersistedFreshDDisks = freshDDisks;
    StartPersist();
    DemoteUnavailableHostsIfNeeded();
    ScheduleCleaningUp();
}

void TVChunk::Touch()
{
    if (TouchedState == ETouchedState::NotTouched) {
        TouchedState = ETouchedState::Persisting;
        DoPersistTouched();
    }
}

bool TVChunk::IsTouched() const
{
    return TouchedState != ETouchedState::NotTouched;
}

void TVChunk::DoPersistTouched()
{
    Y_ABORT_UNLESS(TouchedState == ETouchedState::Persisting);

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Will persist touched",
        LogTitle.GetWithTime().c_str());

    auto future =
        PartitionDirectService->SetVChunkTouched(VChunkConfig.GetVChunkIndex());
    future.Subscribe(
        [weakSelf = weak_from_this(),
         executor = Executor]   //
        (const TPersistResultFuture& f) mutable
        {
            if (f.GetValue() != EPersistResult::Success) {
                return;
            }
            executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf)]()
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnTouchedPersisted();
                    }
                });
        });
}

void TVChunk::OnTouchedPersisted()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Touched persisted.",
        LogTitle.GetWithTime().c_str());

    TouchedState = ETouchedState::Persisted;
    ScheduleCleaningUp();
}

>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
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
        BlocksDirtyMap.NeedFlush() ? "NeedFlush" : "",
        BlocksDirtyMap.NeedErase() ? "NeedErase" : "");

    DoFlush(true);
    DoErase(true, TBlocksDirtyMap::EEraseType::Standard);
<<<<<<< HEAD
=======
    StartPersist();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
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
<<<<<<< HEAD
        .ApplyPersisted = std::move(applyPersisted)});
    PersistNextPendingConfig();
=======
        .Message = std::move(message)});

    StartPersist();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
}

void TVChunk::PersistNextPendingConfig()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

<<<<<<< HEAD
    if (PendingVChunkConfigs.size() != 1) {
        return;
    }
=======
    Y_ABORT_UNLESS(!Persisting);
    Y_ABORT_UNLESS(!PendingVChunkConfigs.empty());
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))

    TPendingVChunkConfig& pending = PendingVChunkConfigs.front();
    auto config = std::move(pending.PrepareConfig)();
    auto message = std::move(pending.Message);
    PendingVChunkConfigs.pop_front();

<<<<<<< HEAD
    pending.Config = std::move(pending.PrepareConfig)();
    Y_ABORT_UNLESS(
        pending.Config.GetVChunkIndex() == VChunkConfig.GetVChunkIndex());
    Y_ABORT_UNLESS(pending.Config.IsValid());

    PartitionDirectService->UpdateVChunkConfig(pending.Config);

    DirectBlockGroup->Schedule(
        TDuration::Seconds(1),
        [weakSelf = weak_from_this()]()
=======
    if (config.Empty() || config == VChunkConfig) {
        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Skip update config: %s %s",
            LogTitle.GetWithTime().c_str(),
            message.Quote().c_str(),
            config.DebugPrint().c_str());

        StartPersist();
        return;
    }

    Persisting = true;

    Y_ABORT_UNLESS(config.GetVChunkIndex() == VChunkConfig.GetVChunkIndex());
    Y_ABORT_UNLESS(config.IsValid());

    const ui32 stateGeneration = BlocksDirtyMap->GetCurrentGeneration();
    auto dirtyMapState = BlocksDirtyMap->MakeFutureState(config, IsTouched());
    // GetOutdatedDDisks() only sees current DDisks and would miss a newly
    // promoted DDisk. Use the state that will be persisted with the config.
    const THostMask freshDDisks = GetFreshDDisks(dirtyMapState);
    auto onPersisted = PartitionDirectService->UpdateVChunkState(
        config,
        std::move(dirtyMapState));
    onPersisted.Subscribe(
        [weakSelf = weak_from_this(),
         executor = Executor,
         config,
         message = std::move(message),
         stateGeneration,
         freshDDisks]   //
        (const TPersistResultFuture& f) mutable
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
        {
            if (auto self = weakSelf.lock()) {
                self->OnConfigPersisted();
            }
<<<<<<< HEAD
=======
            executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf),
                 config,
                 message = std::move(message),
                 stateGeneration,
                 freshDDisks]() mutable
                {
                    if (auto self = weakSelf.lock()) {
                        self->OnConfigPersisted(
                            config,
                            message,
                            stateGeneration,
                            freshDDisks);
                    }
                });
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
        });
}

void TVChunk::OnConfigPersisted(
    const TVChunkConfig& config,
    const TString& message,
    ui32 stateGeneration,
    THostMask freshDDisks)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    Y_ABORT_UNLESS(Persisting);

<<<<<<< HEAD
    auto& pending = *PendingVChunkConfigs.begin();
    VChunkConfig = pending.Config;
    auto apply = std::move(pending.ApplyPersisted);
    PendingVChunkConfigs.pop_front();
    PersistNextPendingConfig();
<<<<<<< HEAD
    apply();
=======
=======
    Persisting = false;

    BlocksDirtyMap->StatePersisted(stateGeneration);
    PersistedFreshDDisks = freshDDisks;
    ApplyConfig(config, message);
    StartPersist();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    DemoteUnavailableHostsIfNeeded();
}

void TVChunk::ApplyConfig(
    const TVChunkConfig& newConfig,
    const TString& message)
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

    VChunkConfig = newConfig;
    BlocksDirtyMap->UpdateConfig(VChunkConfig, IsTouched());

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
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
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
<<<<<<< HEAD
            const TString message = newConfig.EvacuateHost(hostIndex);
=======
            newConfig.DisableHost(hostIndex);
<<<<<<< HEAD
            const TString message = newConfig.PromoteHostIfNeeded(IsTouched());
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
=======
            const TString message = newConfig.PromoteHostIfNeeded();
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
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

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Applying config %s",
        LogTitle.GetWithTime().c_str(),
        VChunkConfig.DebugPrint().c_str());

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

        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Copier %s stopping",
            LogTitle.GetWithTime().c_str(),
            PrintHostIndex(hostIndex).c_str());

        (*copier)->Stop().Subscribe(
            [weakSelf = weak_from_this(), hostIndex]   //
            (const TFuture<TDDiskDataCopier::EResult>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnCopierStopped(hostIndex, f.GetValue());
                }
            });
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

        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Copier %s started",
            LogTitle.GetWithTime().c_str(),
            PrintHostIndex(hostIndex).c_str());

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
        PrintHostIndex(hostIndex).c_str(),
        ToString(result).c_str());
}

void TVChunk::OnCopyComplete(
    THostIndex hostIndex,
    TDDiskDataCopier::EResult result)
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s CopyDDisk %s finished: %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(hostIndex).c_str(),
        ToString(result).c_str());

<<<<<<< HEAD
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
=======
    if (result != TDDiskDataCopier::EResult::Ok) {
        // TODO (drbasic). Decide what to do in case of a copying error.
        Copiers.erase(hostIndex);
        return;
    }

    Copiers.erase(hostIndex);
    StartPersist();
}

void TVChunk::DemoteUnavailableHostsIfNeeded()
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());

    if (GetDDisksForDemote().Empty()) {
        return;
    }

    auto prepare = [weakSelf = weak_from_this()]()
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    {
        if (auto self = weakSelf.lock()) {
            self->ApplyConfig();
        }
    };

<<<<<<< HEAD
    UpdateConfig(std::move(prepare), std::move(apply));
=======
    UpdateConfig(std::move(prepare), "Demote unavailable hosts");
}

THostMask TVChunk::GetDDisksForDemote() const
{
    Y_ABORT_UNLESS(ExecutorThreadChecker.Check());
    auto healthyDDisks = GetHealthyDDisks();
    if (healthyDDisks.Count() < QuorumDirectBlockGroupHostCount) {
        return THostMask::MakeEmpty();
    }

    auto ddiskToDemote = VChunkConfig.GetDDisks()
                             .Exclude(VChunkConfig.GetEnabledDDisks())
                             .Exclude(healthyDDisks);
    return ddiskToDemote;
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
}

void TVChunk::WaitForDirtyMapReady()
{
    if (!DirtyMapReady.HasValue()) {
        const auto dirtyMapReadyFuture = DirtyMapReady.GetFuture();
        Executor->WaitFor(dirtyMapReadyFuture);
    }
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
