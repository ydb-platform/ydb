#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include "partition_direct_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NCloud::NBlockStore;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

TPartitionActor::TPartitionActor(
    const TActorId& owner,
    TTabletStorageInfo* storage,
    const TActorId& volumeActorId,
    ui64 volumeTabletId)
    : TActor<TPartitionActor>(&TThis::StateWork),
      TTabletExecutedFlat(storage, owner, new NMiniKQL::TMiniKQLFactory)
{
    Y_UNUSED(owner);

    // Read worker count from config
    const ui32 configWorkerCount = Config->GetPartitionDirectWorkerCount();
    if (configWorkerCount > 0) {
        WorkerCount = configWorkerCount;
        LOG_INFO_S(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Using worker count from config: " << WorkerCount);
    } else {
        LOG_WARN_S(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Invalid worker count in config: " << configWorkerCount
            << " (must be > 0), using default: " << DEFAULT_WORKER_COUNT);
    }

    // Read worker mode from config
    WorkerMode = Config->GetPartitionDirectWorkerMode();
    LOG_INFO_S(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Using worker mode from config: " << WorkerMode);

    // Initialize storage based on the storage type flag
    TPartitionStoragePtr partitionStorage;
    if (StorageType == NProto::PARTITION_DIRECT_MODE_MEMORY) {
        partitionStorage = std::make_shared<TInMemoryStorage>(PartitionConfig.GetBlockSize());
    } else {
        partitionStorage = std::make_shared<TProxyStorage>(
            PartitionConfig.GetBlockSize(),
            SelfId(),
            State.get());
    }
    State->SetStorage(partitionStorage);
}

TString TPartitionActor::GetStateName(ui32 state)
{
    Y_UNUSED(state);
    return "Work";
}

////////////////////////////////////////////////////////////////////////////////
STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        // External boot
        HFunc(TEvPartition::TEvWaitReadyRequest, HandleWaitReady);

        // PipeCache events
        HFunc(TEvPipeCache::TEvDeliveryProblem, HandleDeliveryProblem);

        // Timer events
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                    "Unhandled event type: " << ev->GetTypeRewrite()
                     << " event: " << ev->ToString());
            }
            break;
    }
}

void TPartitionActor::HandleWaitReady(
    const TEvPartition::TEvWaitReadyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "Received WaitReady request");

    auto response = std::make_unique<TEvPartition::TEvWaitReadyResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TPartitionActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleRetryTimer(ctx);
}

void TPartitionActor::HandleReadBlocksLocalRequest(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Forwarding ReadBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu) to worker",
        TabletID(),
        requestId,
        record.GetStartIndex(),
        record.GetBlocksCount(),
        record.BlockSize);

    // Select worker in round-robin fashion
    auto workerId = SelectNextWorker();
    if (!workerId) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] No workers available for ReadBlocksLocal request #%lu",
            TabletID(), requestId);
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
            MakeError(E_REJECTED, "No workers available"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (!ev->TraceId) {
        // Generate new trace id with throttling to avoid overwhelming the tracing system
        // Uses per-request-type atomic counter to ensure only one trace per sample period
        ev->TraceId = NWilson::TTraceId::NewTraceIdThrottled(
            15,                     // verbosity
            4095,                   // timeToLive
            LastReadTraceTs,        // atomic counter for read requests
            ctx.Monotonic(),        // current monotonic time
            TraceSamplePeriod       // 100ms between samples
        );
    }

    // Forward the entire event to the selected worker
    ctx.Send(ev->Forward(workerId));
}

void TPartitionActor::HandleWriteBlocksLocalRequest(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Forwarding WriteBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu) to worker",
        TabletID(),
        requestId,
        record.GetStartIndex(),
        record.BlocksCount,
        record.BlockSize);

    // Select worker in round-robin fashion
    auto workerId = SelectNextWorker();
    if (!workerId) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] No workers available for WriteBlocksLocal request #%lu",
            TabletID(), requestId);
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            MakeError(E_REJECTED, "No workers available"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (!ev->TraceId) {
        // Generate new trace id with throttling to avoid overwhelming the tracing system
        // Uses per-request-type atomic counter to ensure only one trace per sample period
        ev->TraceId = NWilson::TTraceId::NewTraceIdThrottled(
            15,                     // verbosity
            4095,                   // timeToLive
            LastWriteTraceTs,       // atomic counter for write requests
            ctx.Monotonic(),        // current monotonic time
            TraceSamplePeriod       // 100ms between samples
        );
    }

    // Forward the entire event to the selected worker
    ctx.Send(ev->Forward(workerId));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::Enqueue(STFUNC_SIG)
{
    ALOG_ERROR(TBlockStoreComponents::VOLUME,
        "[" << TabletID() << "]"
        << " IGNORING message type# " << ev->GetTypeRewrite()
        << " from Sender# " << ToString(ev->Sender));
}

void TPartitionActor::DefaultSignalTabletActive(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " DefaultSignalTabletActive - starting InitSchema");

    if (!Executor()->GetStats().IsFollower()) {
        ExecuteTx<TInitSchema>(ctx);
    } else {
        ExecuteTx<TLoadState>(ctx);
    }
}

void TPartitionActor::OnActivateExecutor(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Activated executor");
}

void TPartitionActor::DoActivateExecutor(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Tablet activated, will check virtual group status after loading state");
}

void TPartitionActor::OnDetach(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " OnDetach");

    Die(ctx);
}

void TPartitionActor::OnTabletDead(
    NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " OnTabletDead");

    Die(ctx);
}

void TPartitionActor::HandleDeliveryProblem(
    const TEvPipeCache::TEvDeliveryProblem::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] HandleDeliveryProblem from sender: %lu",
        TabletID(), ev->Sender.LocalId());
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
