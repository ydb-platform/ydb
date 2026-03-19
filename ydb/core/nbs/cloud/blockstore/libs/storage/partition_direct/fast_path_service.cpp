#include "fast_path_service.h"

#include "direct_block_group_in_mem.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/base/counters.h>

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFastPathService::TFastPathService(
    NActors::TActorSystem* actorSystem,
    ui64 tabletId,
    ui32 generation,
    TVector<std::shared_ptr<NStorage::NPartitionDirect::TRegion>> regions,
    const NProto::TStorageServiceConfig& storageConfig,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
    : ActorSystem(actorSystem)
    , Regions(std::move(regions))
    , TraceSamplePeriod(
          TDuration::MilliSeconds(storageConfig.GetTraceSamplePeriod()))
    , Counters(MakeCountersChain(
          std::move(counters),
          storageConfig.GetDDiskPoolName(),
          tabletId))
{
    Y_UNUSED(ActorSystem);
    Y_UNUSED(generation);
}

NWilson::TTraceId TFastPathService::SpanTrace()
{
    return NWilson::TTraceId::NewTraceIdThrottled(
        15,                           // verbosity
        4095,                         // timeToLive
        LastTraceTs,                  // atomic counter for throttling
        NActors::TMonotonic::Now(),   // current monotonic time
        TraceSamplePeriod             // 100ms between samples
    );
}

size_t TFastPathService::GetRegionIndex(ui64 blockIndex) const
{
    return blockIndex / BlocksPerRegion;
}

size_t TFastPathService::GetRegionOffset(ui64 blockIndex) const
{
    return blockIndex % BlocksPerRegion;
}

NThreading::TFuture<TReadBlocksLocalResponse> TFastPathService::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    auto traceId = SpanTrace();

    Counters.RequestStarted(
        EBlockStoreRequest::ReadBlocks,
        request->Range.Size() * DefaultBlockSize);

    const size_t regionIndex = GetRegionIndex(request->Range.Start);
    const ui64 regionOffset = GetRegionOffset(request->Range.Start);
    request->RegionRange =
        TBlockRange64::WithLength(regionOffset, request->Range.Size());

    auto result = Regions[regionIndex]->ReadBlocksLocal(
        std::move(callContext),
        std::move(request),
        std::move(traceId));

    result.Subscribe(
        [weakSelf = weak_from_this()]   //
        (const TFuture<TReadBlocksLocalResponse>& f)
        {
            if (auto self = weakSelf.lock()) {
                self->Counters.RequestFinished(
                    EBlockStoreRequest::ReadBlocks,
                    !HasError(f.GetValue().Error));
            }
        });

    return result;
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TFastPathService::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    auto traceId = SpanTrace();

    Counters.RequestStarted(
        EBlockStoreRequest::WriteBlocks,
        request->Range.Size() * DefaultBlockSize);

    const size_t regionIndex = GetRegionIndex(request->Range.Start);
    const ui64 regionOffset = GetRegionOffset(request->Range.Start);
    request->RegionRange =
        TBlockRange64::WithLength(regionOffset, request->Range.Size());

    auto result = Regions[regionIndex]->WriteBlocksLocal(
        std::move(callContext),
        std::move(request),
        std::move(traceId));

    result.Subscribe(
        [weakSelf = weak_from_this()]   //
        (const TFuture<TWriteBlocksLocalResponse>& f)
        {
            if (auto self = weakSelf.lock()) {
                self->Counters.RequestFinished(
                    EBlockStoreRequest::WriteBlocks,
                    !HasError(f.GetValue().Error));
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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
