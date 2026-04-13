#include "fast_path_service.h"

#include "range_translate.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/base/counters.h>

#include <utility>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

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

TVector<std::shared_ptr<TRegion>> CreateRegions(
    IPartitionDirectService* partitionDirectService,
    ui64 blockCount,
    ui32 blockSize,
    TVector<IDirectBlockGroupPtr> directBlockGroups,
    const TStorageConfig& storageConfig,
    NMonitoring::TDynamicCounterPtr counters)
{
    const ui64 regionsCount =
        AlignUp(blockCount * blockSize, RegionSize) / RegionSize;
    TVector<std::shared_ptr<TRegion>> regions(regionsCount);
    for (size_t i = 0; i < regionsCount; i++) {
        NMonitoring::TDynamicCounterPtr regionCounters =
            counters->GetSubgroup("region", ToString(i));

        regions[i] = std::make_shared<TRegion>(
            TActorContext::ActorSystem(),
            partitionDirectService,
            i,
            directBlockGroups,
            storageConfig.GetSyncRequestsBatchSize(),
            storageConfig.GetVChunkSize(),
            storageConfig.GetWriteHandoffDelay(),
            storageConfig.GetTraceSamplePeriod(),
            regionCounters);
    }

    return regions;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFastPathService::TFastPathService(
    NActors::TActorSystem* actorSystem,
    ui64 tabletId,
    const TString& diskId,
    ui64 blockCount,
    ui32 blockSize,
    TVector<IDirectBlockGroupPtr> directBlockGroups,
    TStorageConfigPtr storageConfig,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
    : ActorSystem(actorSystem)
    , DiskId(diskId)
    , Scheduler(std::move(scheduler))
    , Timer(std::move(timer))
    , Regions(CreateRegions(
          this,
          blockCount,
          blockSize,
          std::move(directBlockGroups),
          *storageConfig,
          MakeCountersChain(
              counters,
              storageConfig->GetDDiskPoolName(),
              tabletId)))
    , TraceSamplePeriod(storageConfig->GetTraceSamplePeriod())
    , Counters(MakeCountersChain(
          std::move(counters),
          storageConfig->GetDDiskPoolName(),
          tabletId))
    , VolumeConfig(std::make_shared<TVolumeConfig>(TVolumeConfig{
          .DiskId = DiskId,
          .BlockSize = blockSize,
          .BlockCount = blockCount,
          .BlocksPerStripe = storageConfig->GetStripeSize(),
          .VChunkSize = storageConfig->GetVChunkSize()}))
    , WriteMode(GetWriteModeFromProto(storageConfig->GetWriteMode()))
    , PBufferReplyTimeout(storageConfig->GetPBufferReplyTimeout())
{}

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

    auto result = Regions[regionIndex]->WriteBlocksLocal(
        std::move(callContext),
        std::move(request),
        WriteMode,
        PBufferReplyTimeout,
        GenerateSequenceNumber(),
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

ui64 TFastPathService::GenerateSequenceNumber()
{
    return ++SequenceGenerator;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
