#include "partition_cleanup_actor.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include <ydb/library/services/services.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

// Handle delete partition request
void TPartitionActor::HandleDeletePartition(
    const TEvService::TEvDeletePartitionRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const bool teardownRunning = !InflightDeleteRequests.empty();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle DeletePartition request (teardownRunning=%d)",
        LogTitle.GetWithTime().c_str(),
        static_cast<int>(teardownRunning));

    InflightDeleteRequests.push_back(
        {.Sender = ev->Sender, .Cookie = ev->Cookie});
    if (!teardownRunning) {
        StartPartitionTeardown(ctx);
    }
}

// Start partition teardown, stop FastPathService first
void TPartitionActor::StartPartitionTeardown(const NActors::TActorContext& ctx)
{
    Become(&TThis::StateDelete);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Become StateDelete",
        LogTitle.GetWithTime().c_str());

    // A request already accepted by BSC can still allocate after our
    // deallocate (residual leak, follow-up).
    if (AddHostInFlight) {
        NTabletPipe::CloseClient(ctx, AddHostInFlight->BSPipeClient);
        AddHostInFlight.reset();
    }

    // Idempotent: no-op when the endpoint was never started.
    GetNbsService()->VhostServer->DetachStorage(GetSocketPath());

    if (LoadActorAdapter) {
        ctx.Send(LoadActorAdapter, new TEvents::TEvPoisonPill());
        LoadActorAdapter = {};
    }

    if (!FastPathService) {
        StartCleanupActor(ctx, DirectBlockGroupsConnections);
        return;
    }

    auto actorSystem = TActivationContext::ActorSystem();
    auto selfId = ctx.SelfID;
    FastPathService->Stop().Subscribe(
        [actorSystem, selfId](const TFuture<void>& f)
        {
            Y_UNUSED(f);
            actorSystem->Send(
                selfId,
                new TEvPartitionDirectPrivate::TEvFastPathServiceStopped());
        });
}

// Fast path service stopped, continue removal of the tablet
void TPartitionActor::HandleFastPathServiceStoppedDuringDelete(
    const TEvPartitionDirectPrivate::TEvFastPathServiceStopped::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s FastPathService stopped, starting cleanup",
        LogTitle.GetWithTime().c_str());

    FastPathService.reset();
    if (CleanupActor || InflightDeleteRequests.empty()) {
        return;
    }
    StartCleanupActor(ctx, DirectBlockGroupsConnections);
}

// Start cleanup actor to make most of the work during delete
void TPartitionActor::StartCleanupActor(
    const NActors::TActorContext& ctx,
    TDirectBlockGroupsConnections connections)
{
    if (CleanupActor) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s StartCleanupActor skipped: cleanup already running %s",
            LogTitle.GetWithTime().c_str(),
            CleanupActor.ToString().c_str());
        return;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s StartCleanupActor groups=%u",
        LogTitle.GetWithTime().c_str(),
        connections.DirectBlockGroupConnectionsSize());

    CleanupActor = ctx.Register(CreatePartitionCleanupActor({
        .Parent = ctx.SelfID,
        .TabletId = TabletID(),
        .Generation = Executor()->Generation(),
        .DiskId = VolumeConfig.GetDiskId(),
        .Connections = std::move(connections),
        .DDiskPoolName = StorageConfig->GetDDiskPoolName(),
        .PersistentBufferDDiskPoolName =
            StorageConfig->GetPersistentBufferDDiskPoolName(),
        .DirectBlockGroupsCount = DirectBlockGroupsCount,
    }));
}

// CleanupActor completed, handle result
void TPartitionActor::HandlePartitionCleanupCompleted(
    const TEvPartitionDirectPrivate::TEvPartitionCleanupCompleted::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (ev->Sender != CleanupActor) {
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Ignore stale cleanup completion from %s (current %s)",
            LogTitle.GetWithTime().c_str(),
            ev->Sender.ToString().c_str(),
            CleanupActor.ToString().c_str());
        return;
    }

    const auto& error = ev->Get()->Error;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s HandlePartitionCleanupCompleted: %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(error).c_str());

    CleanupActor = {};

    if (HasError(error)) {
        const auto replyError = error.GetCode() == E_TIMEOUT
                                    ? error
                                    : MakeError(E_REJECTED, FormatError(error));
        ReplyToDeleteWaiters(ctx, replyError);
    } else {
        FinishDelete(ctx);
    }
}

// Reply to all delete requests
void TPartitionActor::ReplyToDeleteWaiters(
    const NActors::TActorContext& ctx,
    const NProto::TError& error)
{
    TVector<TDeleteWaiter> waiters;
    waiters.swap(InflightDeleteRequests);

    for (const auto& waiter: waiters) {
        auto response =
            std::make_unique<TEvService::TEvDeletePartitionResponse>(error);
        ctx.Send(waiter.Sender, response.release(), 0, waiter.Cookie);
    }
}

// Finish delete, reply to all requests
void TPartitionActor::FinishDelete(const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s FinishDelete",
        LogTitle.GetWithTime().c_str());

    ReplyToDeleteWaiters(ctx, {});
}

// Ignore allocate result during delete
void TPartitionActor::HandleAllocateResultDuringDelete(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Ignore AllocateDDiskBlockGroupResult during delete: %s",
        LogTitle.GetWithTime().c_str(),
        ev->Get()->Record.ShortDebugString().c_str());

    NTabletPipe::CloseAndForgetClient(SelfId(), BSControllerPipeClient);
    if (AddHostInFlight) {
        NTabletPipe::CloseClient(ctx, AddHostInFlight->BSPipeClient);
        AddHostInFlight.reset();
    }
}

// Ignore update volume config during delete
void TPartitionActor::HandleUpdateVolumeConfigDuringDelete(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Reject UpdateVolumeConfig: partition is being deleted",
        LogTitle.GetWithTime().c_str());

    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetStatus(NKikimrBlockStore::ERROR);
    ctx.Send(ev->Sender, response.release());
}

// Ignore update vchunk config during delete
void TPartitionActor::HandleUpdateVChunkConfigDuringDelete(
    const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Drop UpdateVChunkConfig during delete: %s",
        LogTitle.GetWithTime().c_str(),
        ev->Get()->VChunkConfig.DebugPrint().c_str());

    ev->Get()->UpdateCompleted.SetValue(EPersistResult::Cancelled);
}

void TPartitionActor::HandleUpdateDirtyMapStateDuringDelete(
    const TEvPartitionDirectPrivate::TEvUpdateDirtyMapState::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Drop UpdateDirtyMapState during delete: vchunk %u",
        LogTitle.GetWithTime().c_str(),
        ev->Get()->VChunkIndex);

    ev->Get()->UpdateCompleted.SetValue(EPersistResult::Cancelled);
}

// Ignore fast path service shutdown during delete
void TPartitionActor::HandleFastPathServiceShutdownDuringDelete(
    const TEvPartitionDirectPrivate::TEvFastPathServiceShutdown::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s FastPathServiceShutdown during delete",
        LogTitle.GetWithTime().c_str());

    Reply(
        ctx,
        *ev,
        std::make_unique<
            TEvPartitionDirectPrivate::TEvFastPathServiceStopped>());
}

// Ignore add host to dbg during delete
void TPartitionActor::HandleAddHostToDBGDuringDelete(
    const TEvPartitionDirectPrivate::TEvAddHostToDBG::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const size_t dbgId = ev->Get()->DirectBlockGroupId;
    if (FastPathService) {
        RejectAddHost(ctx, dbgId, "partition is being deleted");
        return;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Drop AddHost during delete (dbgId=%lu): FastPathService stopped",
        LogTitle.GetWithTime().c_str(),
        dbgId);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateDelete)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Processing event: %s from sender: %lu",
        LogTitle.GetWithTime().c_str(),
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        // One more request to delete partition while we are removing the tablet
        HFunc(TEvService::TEvDeletePartitionRequest, HandleDeletePartition);
        // Fast path service stopped, continue teardown
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceStopped,
            HandleFastPathServiceStoppedDuringDelete);
        // Cleanup completed, finish delete
        HFunc(
            TEvPartitionDirectPrivate::TEvPartitionCleanupCompleted,
            HandlePartitionCleanupCompleted);
        // Ignore allocate result during delete
        HFunc(
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
            HandleAllocateResultDuringDelete);
        // Ignore update volume config during delete
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfigDuringDelete);
        // Ignore update vchunk config during delete
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateVChunkConfig,
            HandleUpdateVChunkConfigDuringDelete);
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateDirtyMapState,
            HandleUpdateDirtyMapStateDuringDelete);
        // Ignore fast path service shutdown during delete
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceShutdown,
            HandleFastPathServiceShutdownDuringDelete);
        // Ignore add host to dbg during delete
        HFunc(
            TEvPartitionDirectPrivate::TEvAddHostToDBG,
            HandleAddHostToDBGDuringDelete);
        // The Run() future is not cancelled by Stop(); ignore a late ready
        // signal
        IgnoreFunc(TEvPartitionDirectPrivate::TEvFastPathServiceReady);
        default:
            HandleCommonEvents(ev);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
