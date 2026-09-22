#include "fast_path_service.h"

#include "direct_block_group.h"
#include "partition_direct_events_private.h"
#include "range_translate.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/base/counters.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <library/cpp/threading/future/wait/wait.h>

#include <util/system/fs.h>

#include <memory>
#include <utility>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

void DumpToFile(
    const TString& diskId,
    size_t index,
    const TString& config,
    TMap<size_t, TDBGDumpResponse> debugDumps)
{
    TVector<TDBGDumpResponse::TVChunkDump> dumps;
    for (auto& [dbgIndex, dump]: debugDumps) {
        for (auto& vchunkDump: dump.Dumps) {
            dumps.push_back(std::move(vchunkDump));
        }
    }

    Sort(
        dumps,
        [](const TDBGDumpResponse::TVChunkDump& lhs,
           const TDBGDumpResponse::TVChunkDump& rhs)
        {
            return lhs.VChunkConfig.GetVChunkIndex() <
                   rhs.VChunkConfig.GetVChunkIndex();
        });

    auto dirPath = TString("/tmp/dirty_map/");
    NFs::MakeDirectoryRecursive(dirPath);

    auto path = TStringBuilder() << dirPath << diskId << "." << index;
    TFile file(path, EOpenModeFlag::CreateAlways);

    file.Write(config.data(), config.size());
    file.Write("\n", 1);

    for (const auto& [dbgIndex, dump]: debugDumps) {
        file.Write(dump.Dump.data(), dump.Dump.size());
    }

    for (const auto& dump: dumps) {
        file.Write(dump.Dump.data(), dump.Dump.size());
    }
}

NMonitoring::TDynamicCounterPtr MakeCountersChain(
    NMonitoring::TDynamicCounterPtr counters,
    const TString& ddiskPool,
    ui64 tabletId)
{
    if (!counters) {
        return nullptr;
    }

    NMonitoring::TDynamicCounterPtr result =
        GetServiceCounters(std::move(counters), "nbs_partitions");
    result = result->GetSubgroup("ddiskPool", ddiskPool);
    result = result->GetSubgroup("tabletId", ToString(tabletId));
    result = result->GetSubgroup("subsystem", "interface");
    return result;
}

size_t RegionCount(ui64 blockCount, ui32 blockSize)
{
    return AlignUp(blockCount * blockSize, RegionSize) / RegionSize;
}

TVector<TRegionPtr> CreateRegions(
<<<<<<< HEAD
<<<<<<< HEAD
=======
    IArenaAllocatorPtr arenaAllocator,
=======
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    ITraceService* traceService,
>>>>>>> 78a20188184 (Field map set optimization (#52565))
    IPartitionDirectService* partitionDirectService,
    ui64 blockCount,
    ui32 blockSize,
    const TVector<IDirectBlockGroupPtr>& directBlockGroups,
<<<<<<< HEAD
    const TVChunkConfigByIndex& vChunkConfigs,
    const TStorageConfig& storageConfig,
    NMonitoring::TDynamicCounterPtr counters)
=======
    const TVChunkConfigs& vChunkConfigs,
    ITouchedProvider* touchedProvider,
    const TDirtyMapStateProtos& dirtyMapStates,
    const TStorageConfig& storageConfig)
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
{
<<<<<<< HEAD
    const size_t regionCount = RegionCount(blockCount, blockSize);
=======
    blockSize = CheckedBlockSize(blockSize, storageConfig);
    const size_t regionCount =
        GetRegionCount(blockCount, blockSize, storageConfig.GetVChunkSize());
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
    TVector<TRegionPtr> regions(regionCount);
    for (size_t i = 0; i < regionCount; i++) {
        NMonitoring::TDynamicCounterPtr regionCounters =
            counters->GetSubgroup("region", ToString(i));

        regions[i] = std::make_shared<TRegion>(
            TActorContext::ActorSystem(),
            partitionDirectService,
            i,
            directBlockGroups,
            vChunkConfigs,
<<<<<<< HEAD
<<<<<<< HEAD
=======
            touchedProvider->GetTouchedVChunks(i * VChunkPerRegionCount),
=======
            touchedProvider->GetTouchedVChunks(i),
>>>>>>> 7af514ad642 ([NBS-7516] Extend the partition direct overview page with actual storage placement for touched VChunks (#53472))
            dirtyMapStates,
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
            storageConfig.GetSyncRequestsBatchSize(),
            storageConfig.GetVChunkSize(),
            regionCounters);
    }

    return regions;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFastPathService::TFastPathService(
    NActors::TActorSystem* actorSystem,
    NActors::TActorId partitionActorId,
    ui64 tabletId,
    const TString& diskId,
    ui64 blockCount,
    ui32 blockSize,
    TVector<IDirectBlockGroupPtr> directBlockGroups,
<<<<<<< HEAD
    TVChunkConfigByIndex vChunkConfigs,
=======
    TVector<NTransport::IChaosInjectorControlPtr> chaosInjectorControls,
    const TVChunkConfigs& vChunkConfigs,
    ITouchedProvider* touchedProvider,
    const TDirtyMapStateProtos& dirtyMapStates,
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
    TStorageConfigPtr storageConfig,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
    : ActorSystem(actorSystem)
    , PartitionActorId(partitionActorId)
    , StorageConfig(std::move(storageConfig))
    , DiskId(diskId)
    , Scheduler(std::move(scheduler))
    , Timer(std::move(timer))
    , DirectBlockGroups(std::move(directBlockGroups))
<<<<<<< HEAD
=======
    , ArenaAllocator(
          DirectBlockGroups.front()->GetArenaAllocatorPool()->GetAllocator())
    , ChaosInjectorControls(std::move(chaosInjectorControls))
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    , Regions(CreateRegions(
          this,
          blockCount,
          blockSize,
          DirectBlockGroups,
          vChunkConfigs,
<<<<<<< HEAD
          *StorageConfig,
          MakeCountersChain(
              counters,
              StorageConfig->GetDDiskPoolName(),
              tabletId)))
=======
          touchedProvider,
          dirtyMapStates,
          *StorageConfig))
    , LogTitle(
          GetCycleCount(),
          TLogTitle::TFastPathService{
              .DiskId = DiskDescription.DiskId,
              .TabletId = DiskDescription.TabletId,
              .Generation = DiskDescription.Generation})
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
    , TraceSamplePeriod(StorageConfig->GetTraceSamplePeriod())
    , Counters(MakeCountersChain(
          std::move(counters),
          StorageConfig->GetDDiskPoolName(),
          tabletId))
    , VolumeConfig(std::make_shared<TVolumeConfig>(TVolumeConfig{
          .DiskId = DiskId,
          .BlockSize = blockSize,
          .BlockCount = blockCount,
          .BlocksPerStripe = StorageConfig->GetStripeSize() / blockSize,
          .VChunkSize = StorageConfig->GetVChunkSize()}))
<<<<<<< HEAD
{}
=======
{
    Y_ABORT_UNLESS(DirectBlockGroups.size() == ChaosInjectorControls.size());
    Y_ABORT_UNLESS(ArenaAllocator);

    const ui64 copyRangeBandwidth =
        StorageConfig->GetCopyRangeBandwidthMbs() * 1_MB;
    if (copyRangeBandwidth) {
        CopyRangeBucket.emplace(
            ActorSystem->Timestamp(),
            copyRangeBandwidth,
            copyRangeBandwidth);
    }

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Regions: %zu",
        LogTitle.GetWithTime().c_str(),
        Regions.size());
}
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

TFastPathService::~TFastPathService()
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TFastPathService::Destroy %s",
        DiskId.Quote().c_str());
}

NThreading::TFuture<void> TFastPathService::Run()
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TFastPathService::Run %s",
        DiskId.Quote().c_str());

    TVector<NThreading::TFuture<void>> initialReadyFutures;
    initialReadyFutures.reserve(DirectBlockGroups.size());
    for (const auto& dbg: DirectBlockGroups) {
        initialReadyFutures.push_back(dbg->Run(this));
    }
    for (const auto& region: Regions) {
        region->Run();
    }
    ScheduleDirtyMapDebugPrint();

    return NThreading::WaitAll(initialReadyFutures);
}

NThreading::TFuture<void> TFastPathService::Stop()
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TFastPathService::Stop %s",
        DiskId.Quote().c_str());

    TVector<NThreading::TFuture<void>> stopFutures;
    for (const auto& region: Regions) {
        stopFutures.push_back(region->Stop());
    }

    return NThreading::WaitAll(stopFutures);
}

NThreading::TFuture<TReadBlocksLocalResponse> TFastPathService::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    auto span = std::make_shared<NWilson::TSpan>(NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        callContext->RootTraceId.Clone(),
        "FastPath.Read",
        NWilson::EFlags::AUTO_END,
        ActorSystem));

    Counters.RequestStarted(
        EBlockStoreRequest::ReadBlocks,
        request->Headers.GetRequestSize());

    const size_t regionIndex =
        GetRegionIndex(*request->Headers.VolumeConfig, request->Headers.Range);

    Y_ABORT_UNLESS(
        regionIndex < Regions.size(),
        "Region index out of bound: %" PRISZT " >= %" PRISZT,
        regionIndex,
        Regions.size());

    auto result = Regions[regionIndex]->ReadBlocksLocal(
        std::move(callContext),
        std::move(request),
        span->GetTraceId());

    result.Subscribe(
        [weakSelf = weak_from_this(), span = std::move(span)]   //
        (const TFuture<TReadBlocksLocalResponse>& f)
        {
            const auto& response = f.GetValue();
            if (HasError(response.Error)) {
                span->EndError(FormatError(response.Error));
            }

            if (auto self = weakSelf.lock()) {
                self->Counters.RequestFinished(
                    EBlockStoreRequest::ReadBlocks,
                    !HasError(response.Error));
            }
        });

    return result;
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TFastPathService::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    auto span = std::make_shared<NWilson::TSpan>(NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        callContext->RootTraceId.Clone(),
        "FastPath.Write",
        NWilson::EFlags::AUTO_END,
        ActorSystem));

    Counters.RequestStarted(
        EBlockStoreRequest::WriteBlocks,
        request->Headers.GetRequestSize());

    const size_t regionIndex =
        GetRegionIndex(*request->Headers.VolumeConfig, request->Headers.Range);

    Y_ABORT_UNLESS(
        regionIndex < Regions.size(),
        "Region index out of bound: %" PRISZT " >= %" PRISZT,
        regionIndex,
        Regions.size());

    auto result = Regions[regionIndex]->WriteBlocksLocal(
        std::move(callContext),
        std::move(request),
        span->GetTraceId());

    result.Subscribe(
        [weakSelf = weak_from_this(), span = std::move(span)]   //
        (const TFuture<TWriteBlocksLocalResponse>& f)
        {
            const auto& response = f.GetValue();
            if (HasError(response.Error)) {
                span->EndError(FormatError(response.Error));
            }

            if (auto self = weakSelf.lock()) {
                self->Counters.RequestFinished(
                    EBlockStoreRequest::WriteBlocks,
                    !HasError(response.Error));
            }
        });

    return result;
}

NThreading::TFuture<TZeroBlocksLocalResponse> TFastPathService::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);
    Y_UNUSED(request);
    Y_ABORT_UNLESS(false, "ZeroBlocksLocal is not implemented");
    return NThreading::MakeFuture<TZeroBlocksLocalResponse>();
}

void TFastPathService::ReportIOError()
{
    // TODO: implement
}

TVolumeConfigPtr TFastPathService::GetVolumeConfig() const
{
    return VolumeConfig;
}

NWilson::TSpan TFastPathService::CreteRootSpan(TStringBuf name)
{
    auto traceId = NWilson::TTraceId::NewTraceIdThrottled(
        NKikimr::TWilsonNbs::NbsBasic,   // verbosity
        4095,                            // timeToLive
        LastTraceTs,                     // atomic counter for throttling
        NActors::TMonotonic::Now(),      // current monotonic time
        TraceSamplePeriod                // 100ms between samples
    );

    return NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        std::move(traceId),
        name.data(),
        NWilson::EFlags::AUTO_END,
        ActorSystem);
}

void TFastPathService::ScheduleAfterDelay(
    TExecutorPtr executor,
    TDuration delay,
    TCallback callback)
{
    Scheduler->Schedule(
        executor.get(),
        Timer->Now() + delay,
        std::move(callback));
}

void TFastPathService::UpdateVChunkConfig(const TVChunkConfig& cfg)
{
    auto event =
        std::make_unique<TEvPartitionDirectPrivate::TEvUpdateVChunkConfig>(cfg);
    ActorSystem->Send(PartitionActorId, event.release());
}

<<<<<<< HEAD
void TFastPathService::RequestAddHost(size_t directBlockGroupId)
=======
TPersistResultFuture TFastPathService::UpdateDirtyMapState(
    ui32 vChunkIndex,
    TDirtyMapStateProto state)
{
    auto event =
        std::make_unique<TEvPartitionDirectPrivate::TEvUpdateDirtyMapState>(
            vChunkIndex,
            std::move(state));
    auto result = event->UpdateCompleted.GetFuture();
    ActorSystem->Send(PartitionActorId, event.release());
    return result;
}

TPersistResultFuture TFastPathService::SetVChunkTouched(ui32 vChunkIndex)
{
    auto event =
        std::make_unique<TEvPartitionDirectPrivate::TEvSetVChunkTouched>(
            vChunkIndex);
    auto result = event->UpdateCompleted.GetFuture();
    ActorSystem->Send(PartitionActorId, event.release());
    return result;
}

void TFastPathService::QueryAddHost(
    size_t directBlockGroupId,
    ui32 dbgConnectionsConfigGeneration)
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
{
    auto event = std::make_unique<TEvPartitionDirectPrivate::TEvAddHostToDBG>(
        directBlockGroupId);
    ActorSystem->Send(PartitionActorId, event.release());
}

void TFastPathService::QueryRemoveHost(
    size_t directBlockGroupId,
    size_t hostIndex,
    ui32 dbgConnectionsConfigGeneration)
{
    auto event =
        std::make_unique<TEvPartitionDirectPrivate::TEvRemoveHostFromDBG>(
            directBlockGroupId,
            hostIndex,
            dbgConnectionsConfigGeneration);
    ActorSystem->Send(PartitionActorId, event.release());
}

ui64 TFastPathService::OnWriteStarted()
{
    ++InflightWriteCount;
    return ++SequenceGenerator;
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
void TFastPathService::OnWriteFinished()
{
    const size_t previous = InflightWriteCount.fetch_sub(1);
    Y_ABORT_UNLESS(previous > 0);
}

size_t TFastPathService::GetInflightWriteCount() const
{
    return InflightWriteCount.load();
}

>>>>>>> 67e4af6a66a ([NBS-6826] Dynamic selection of write mode (#53486))
void TFastPathService::StopTablet(const TString& reason)
{
    // Just forward the signal to the actor thread.
    auto event = std::make_unique<TEvPartitionDirectPrivate::TEvPoison>(reason);
    ActorSystem->Send(PartitionActorId, event.release());
}

TDuration TFastPathService::TakeVolumeCopyRangeBudget(ui64 byteCount)
{
    if (!CopyRangeBucket) {
        return {};
    }

    auto guard = Guard(CopyRangeBucketLock);
    return CopyRangeBucket->Register(ActorSystem->Timestamp(), byteCount);
}

void TFastPathService::PersistHostHealth(
    size_t directBlockGroupId,
    THostIndex hostIndex,
    EHostHealth oldHealth,
    EHostHealth newHealth)
{
    auto event =
        std::make_unique<TEvPartitionDirectPrivate::TEvPersistHostHealth>(
            directBlockGroupId,
            hostIndex,
            oldHealth,
            newHealth);
    ActorSystem->Send(PartitionActorId, event.release());
}

>>>>>>> 8a60344d593 (NBS-7513:  Add Oracle->PartitionDirect PersistHostHealth path (#52610))
TFastPathServiceInfo TFastPathService::GetMonInfo() const
{
    const ui64 vchunkSize = StorageConfig->GetVChunkSize();
    Y_ABORT_UNLESS(vchunkSize != 0);
    return {
        .LsnCounter = SequenceGenerator.load(),
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        .TotalVChunks = Regions.size() * (RegionSize / vchunkSize),
        .DbgCount = DirectBlockGroups.size(),
=======
        .LastSafeBarrier = LastSafeBarrier.load(),
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
=======
>>>>>>> 3b287e3de3d (NBS-7459: own PBuffer cleanup in DirectBlockGroup (#52414))
=======
        .InflightWriteCount = InflightWriteCount.load(),
>>>>>>> 67e4af6a66a ([NBS-6826] Dynamic selection of write mode (#53486))
        .ArenaMemoryUsage = {.Slots = ArenaAllocator->GetDetailedStat()},
    };
}

NThreading::TFuture<TVector<TDbgSnapshot>> TFastPathService::GatherMonSnapshots(
    std::optional<size_t> dbgIndex) const
{
    TVector<NThreading::TFuture<TDbgSnapshot>> futures;
    if (dbgIndex) {
        if (*dbgIndex < DirectBlockGroups.size()) {
            futures.push_back(DirectBlockGroups[*dbgIndex]->BuildMonSnapshot());
        }
    } else {
        for (const auto& dbg: DirectBlockGroups) {
            futures.push_back(dbg->BuildMonSnapshot());
        }
    }

    return NThreading::WaitAll(futures).Apply(
        [futures](const auto&)
        {
            TVector<TDbgSnapshot> snapshots;
            snapshots.reserve(futures.size());
            for (const auto& future: futures) {
                snapshots.push_back(future.GetValue());
            }
            return snapshots;
        });
}

<<<<<<< HEAD
=======
NThreading::TFuture<std::optional<TVChunkSnapshot>>
TFastPathService::GatherVChunkMonSnapshot(ui32 vchunkIndex) const
{
    const auto notFound =
        MakeFuture<std::optional<TVChunkSnapshot>>(std::nullopt);

    const size_t regionIndex = GetRegionIndexByVChunk(vchunkIndex);
    if (regionIndex >= Regions.size()) {
        return notFound;
    }
    const size_t vChunkIndexInRegion = GetVChunkIndexInRegion(vchunkIndex);
    auto vchunk = Regions[regionIndex]->GetVChunk(vChunkIndexInRegion);
    if (!vchunk) {
        return notFound;
    }

    // The vchunk state is confined to its executor (the one of its DBG).
    auto executor = vchunk->GetExecutor();
    auto promise = NewPromise<std::optional<TVChunkSnapshot>>();
    auto future = promise.GetFuture();
    executor->ExecuteSimple([vchunk = std::move(vchunk), promise]() mutable
                            { promise.SetValue(vchunk->BuildMonSnapshot()); });
    return future;
}

NThreading::TFuture<TVChunkStatsGatherResult>
TFastPathService::GatherVChunkStats(
    EVChunkStatsDetail detail,
    std::optional<size_t> dbgIndex) const
{
    TVector<NThreading::TFuture<TVChunkStatsGatherResult>> futures;
    futures.reserve(DirectBlockGroups.size());
    for (size_t i = 0; i < DirectBlockGroups.size(); ++i) {
        const auto dbgDetail = (detail == EVChunkStatsDetail::PerVChunk &&
                                (!dbgIndex || *dbgIndex == i))
                                   ? EVChunkStatsDetail::PerVChunk
                                   : EVChunkStatsDetail::TotalOnly;
        futures.push_back(DirectBlockGroups[i]->GatherVChunkStats(dbgDetail));
    }

    return NThreading::WaitAll(futures).Apply(
        [futures](const auto&)
        {
            TVChunkStatsGatherResult result;
            result.PerDbg.reserve(futures.size());
            for (const auto& future: futures) {
                const auto& part = future.GetValue();
                result.Total.Accumulate(part.Total);
                result.PerDbg.push_back(TVChunkDbgStats{
                    .DbgIndex = part.DbgIndex,
                    .Stats = part.Total,
                });
                result.PerVChunk.insert(
                    result.PerVChunk.end(),
                    part.PerVChunk.begin(),
                    part.PerVChunk.end());
            }
            return result;
        });
}

void TFastPathService::OnRegionStopped(size_t regionIndex)
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnRegionStopped %zu",
        LogTitle.GetWithTime().c_str(),
        regionIndex);
}

void TFastPathService::OnAllRegionsStopped()
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnAllRegionsStopped",
        LogTitle.GetWithTime().c_str());
}

<<<<<<< HEAD
>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
void TFastPathService::MaybeTriggerPBufferCleanup(ui64 lsn)
{
    const ui64 step = StorageConfig->GetPBufferCleanupLsnStep();
    if (!step || lsn % step != 0) {
        return;
    }
    // lsn values are unique, so exactly one generator hits each multiple of
    // step. Skip if a previous cleanup is still gathering, to avoid overlap.
    bool expected = false;
    if (!CleanupGather.Active.compare_exchange_strong(expected, true)) {
        return;
    }
    PBufferCleanup();
}

void TFastPathService::PBufferCleanup()
{
    // Pull the smallest inflight lsn from every DirectBlockGroup. Each group
    // writes its own result slot; the last responder computes the global
    // minimum (see FinishPBufferCleanup).
    const size_t dbgCount = DirectBlockGroups.size();
    CleanupGather.SafeBarriers.assign(dbgCount, std::nullopt);
    CleanupGather.PendingResponses.store(dbgCount);

    for (size_t i = 0; i < dbgCount; ++i) {
        DirectBlockGroups[i]->GatherSafeBarrierForErase().Subscribe(
            [weakSelf = weak_from_this(), i]   //
            (const NThreading::TFuture<std::optional<ui64>>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnGatherSafeBarrierForErase(i, f.GetValue());
                }
            });
    }
}

void TFastPathService::OnGatherSafeBarrierForErase(
    size_t dbgIndex,
    std::optional<ui64> safeBarrier)
{
    CleanupGather.SafeBarriers[dbgIndex] = safeBarrier;
    if (CleanupGather.PendingResponses.fetch_sub(1) == 1) {
        FinishPBufferCleanup();
    }
}

void TFastPathService::FinishPBufferCleanup()
{
    std::optional<ui64> globalMin;
    for (const auto& safeBarrier: CleanupGather.SafeBarriers) {
        if (safeBarrier && (!globalMin || *safeBarrier < *globalMin)) {
            globalMin = safeBarrier;
        }
    }

    CleanupGather.Active.store(false);

    if (!globalMin || *globalMin == 0) {
        // 0 is the blocking bound: some vchunk has not finished restoring its
        // dirty map, so its records are not accounted for yet. Skip the tick.
        return;
    }

    const ui64 cleanupBound = *globalMin - 1;
    for (const auto& dbg: DirectBlockGroups) {
        dbg->BarrierEraseFromPBuffer(cleanupBound);
    }
}

=======
>>>>>>> 3b287e3de3d (NBS-7459: own PBuffer cleanup in DirectBlockGroup (#52414))
void TFastPathService::ScheduleDirtyMapDebugPrint()
{
    auto delay = StorageConfig->GetDirtyMapDebugPrintInterval();

    if (!delay) {
        return;
    }

    ScheduleAfterDelay(
        nullptr,
        delay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->QueryDirtyMapDebugDump();
            }
        });
}

void TFastPathService::QueryDirtyMapDebugDump()
{
    size_t dbgIndex = 0;
    for (const auto& dbg: DirectBlockGroups) {
        auto response = dbg->Dump();
        response.Subscribe(
            [weakSelf = weak_from_this(), dbgIndex](TFuture<TDBGDumpResponse> f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnDebugDump(dbgIndex, UnsafeExtractValue(f));
                }
            });
        ++dbgIndex;
    }
}

void TFastPathService::OnDebugDump(size_t dbgIndex, TDBGDumpResponse dump)
{
    auto guard = Guard(DumpLock);

    DebugDumps[dbgIndex] = std::move(dump);
    if (DebugDumps.size() != DirectBlockGroups.size()) {
        return;
    }

    try {
        DumpToFile(
            DiskId,
            DumpCount,
            StorageConfig->Dump(),
            std::move(DebugDumps));
    } catch (const std::exception& e) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "Dump error %s",
            e.what());
    }

    ScheduleDirtyMapDebugPrint();
    ++DumpCount;
}

<<<<<<< HEAD
=======
void TFastPathService::ScheduleVChunkCountersUpdate()
{
    auto delay = StorageConfig->GetVChunkCountersUpdateInterval();
    if (!delay || !Scheduler) {
        return;
    }

    ScheduleAfterDelay(
        nullptr,
        delay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->QueryVChunkStats();
            }
        });
}

void TFastPathService::QueryVChunkStats()
{
    GatherVChunkStats(EVChunkStatsDetail::TotalOnly)
        .Subscribe(
            [weakSelf = weak_from_this()](const auto& future)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnVChunkStats(future.GetValue());
                }
            });
}

void TFastPathService::OnVChunkStats(const TVChunkStatsGatherResult& result)
{
    VChunkCounters.Publish(result.Total);
    ScheduleVChunkCountersUpdate();
}

>>>>>>> f32af13259a (Refactor partition direct disk geometry calculations (#53169))
////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
