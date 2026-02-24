#include "fast_path_service.h"

#include "direct_block_group_in_mem.h"

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/base/counters.h>

using namespace NKikimr;
using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;

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
        GetServiceCounters(counters, "nbs_partitions");
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
    TVector<NBsController::TDDiskId> ddiskIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds,
    ui32 blockSize,
    ui64 blocksCount,
    ui32 storageMedia,
    const NProto::TStorageServiceConfig& storageConfig,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
    : ActorSystem(actorSystem)
    , TraceSamplePeriod(
          TDuration::MilliSeconds(storageConfig.GetTraceSamplePeriod()))
    , Counters(MakeCountersChain(
          std::move(counters),
          storageConfig.GetDDiskPoolName(),
          tabletId))
{
    if (storageMedia == NProto::EStorageMediaKind::STORAGE_MEDIA_MEMORY) {
        DirectBlockGroup = std::make_shared<
            NStorage::NPartitionDirect::TInMemoryDirectBlockGroup>(
            tabletId,
            generation,
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds),
            blockSize,
            blocksCount);
    } else {
        DirectBlockGroup =
            std::make_shared<NStorage::NPartitionDirect::TDirectBlockGroup>(
                ActorSystem,
                tabletId,
                generation,
                std::move(ddiskIds),
                std::move(persistentBufferDDiskIds),
                blockSize,
                blocksCount);
    }

    DirectBlockGroup->EstablishConnections(SpanTrace());
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

NThreading::TFuture<TReadBlocksLocalResponse> TFastPathService::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    with_lock (Lock) {
        auto traceId = SpanTrace();

        Counters.RequestStarted(
            EBlockStoreRequest::ReadBlocks,
            request->Range.Size() * BlockSize);

        auto result = DirectBlockGroup->ReadBlocksLocal(
            std::move(callContext),
            std::move(request),
            std::move(traceId));

        result.Subscribe(
            [weakSelf =
                 weak_from_this()](const TFuture<TReadBlocksLocalResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->Counters.RequestFinished(
                        EBlockStoreRequest::ReadBlocks,
                        !HasError(f.GetValue().Error));
                }
            });

        return result;
    }
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TFastPathService::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    with_lock (Lock) {
        auto traceId = NWilson::TTraceId::NewTraceIdThrottled(
            15,                           // verbosity
            4095,                         // timeToLive
            LastTraceTs,                  // atomic counter for throttling
            NActors::TMonotonic::Now(),   // current monotonic time
            TraceSamplePeriod             // 100ms between samples
        );

        Counters.RequestStarted(
            EBlockStoreRequest::WriteBlocks,
            request->Range.Size() * BlockSize);

        auto result = DirectBlockGroup->WriteBlocksLocal(
            std::move(callContext),
            std::move(request),
            std::move(traceId));

        result.Subscribe(
            [weakSelf =
                 weak_from_this()](const TFuture<TWriteBlocksLocalResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->Counters.RequestFinished(
                        EBlockStoreRequest::WriteBlocks,
                        !HasError(f.GetValue().Error));
                }
            });

        return result;
    }
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
