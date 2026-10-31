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

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/threading/future/future.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

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

    YDB_LOG_INFO_CTX(ctx, "Handle DeletePartition request",
        {"withTime", LogTitle.GetWithTime()},
        {"teardownRunning", static_cast<int>(teardownRunning)});

    InflightDeleteRequests.push_back(
        {.Sender = ev->Sender, .Cookie = ev->Cookie});
    if (!teardownRunning) {
        StartPartitionTeardown(ctx);
    }
}

// Start partition teardown, stop FastPathService first
void TPartitionActor::StartPartitionTeardown(const NActors::TActorContext& ctx)
{
    UnregisterFrontendVolume(ctx);
    Become(&TThis::StateDelete);

    YDB_LOG_INFO_CTX(ctx, "Become StateDelete",
        {"withTime", LogTitle.GetWithTime()});

    StopBscProxy(ctx);
    AddHostInFlight.reset();
    RemoveHostInFlight.reset();

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

    YDB_LOG_INFO_CTX(ctx, "FastPathService stopped, starting cleanup",
        {"withTime", LogTitle.GetWithTime()});

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
        YDB_LOG_ERROR_CTX(ctx, "StartCleanupActor skipped: cleanup already running",
            {"withTime", LogTitle.GetWithTime()},
            {"cleanupActor", CleanupActor});
        return;
    }

    YDB_LOG_INFO_CTX(ctx, "StartCleanupActor",
        {"withTime", LogTitle.GetWithTime()},
        {"groups", connections.DirectBlockGroupConnectionsSize()});

    CleanupActor = ctx.Register(CreatePartitionCleanupActor({
        .Parent = ctx.SelfID,
        .TabletId = TabletID(),
        .Generation = Executor()->Generation(),
        .DiskId = VolumeConfig.GetDiskId(),
        .Connections = std::move(connections),
        .DDiskPoolName = StorageConfig->GetDDiskPoolName(),
        .PersistentBufferDDiskPoolName =
            StorageConfig->GetPersistentBufferDDiskPoolName(),
        .DirectBlockGroupsCount = DefaultVolumeDirectBlockGroupCount,
    }));
}

// CleanupActor completed, handle result
void TPartitionActor::HandlePartitionCleanupCompleted(
    const TEvPartitionDirectPrivate::TEvPartitionCleanupCompleted::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (ev->Sender != CleanupActor) {
        YDB_LOG_WARN_CTX(ctx, "Ignore stale cleanup completion from (current",
            {"withTime", LogTitle.GetWithTime()},
            {"sender", ev->Sender},
            {"cleanupActor", CleanupActor});
        return;
    }

    const auto& error = ev->Get()->Error;

    YDB_LOG_INFO_CTX(ctx, "",
        {"withTime", LogTitle.GetWithTime()},
        {"handlePartitionCleanupCompleted", FormatError(error)});

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
    YDB_LOG_INFO_CTX(ctx, "FinishDelete",
        {"withTime", LogTitle.GetWithTime()});

    ReplyToDeleteWaiters(ctx, {});
}

// Ignore allocate result during delete
void TPartitionActor::HandleAllocateResultDuringDelete(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "Ignore AllocateDDiskBlockGroupResult during",
        {"withTime", LogTitle.GetWithTime()},
        {"delete", ev->Get()->Record.ShortDebugString()});
}

// Ignore update volume config during delete
void TPartitionActor::HandleUpdateVolumeConfigDuringDelete(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "Reject UpdateVolumeConfig: partition is being deleted",
        {"withTime", LogTitle.GetWithTime()});

    // Schemeshard aborts on any status other than OK or
    // ERROR_UPDATE_IN_PROGRESS. The partition cannot apply a new config while
    // it is tearing down, so report that the update is still in progress.
    ReplyUpdateVolumeConfig(
        ctx,
        ev,
        NKikimrBlockStore::ERROR_UPDATE_IN_PROGRESS);
}

void TPartitionActor::HandleStatVolumeDuringDelete(
    const NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeRequest::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "Reject StatVolume: partition is being deleted",
        {"withTime", LogTitle.GetWithTime()});

    auto response = std::make_unique<
        NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeResponse>(
        MakeError(E_REJECTED, "partition is being deleted"));
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

// Ignore update vchunk config during delete
void TPartitionActor::HandleUpdateVChunkConfigDuringDelete(
    const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "Drop UpdateVChunkConfig during",
        {"withTime", LogTitle.GetWithTime()},
        {"delete", ev->Get()->VChunkConfig.DebugPrint()});

    ev->Get()->UpdateCompleted.SetValue(EPersistResult::Cancelled);
}

void TPartitionActor::HandleUpdateDirtyMapStateDuringDelete(
    const TEvPartitionDirectPrivate::TEvUpdateDirtyMapState::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "Drop UpdateDirtyMapState during delete: vchunk",
        {"withTime", LogTitle.GetWithTime()},
        {"VChunkIndex", ev->Get()->VChunkIndex});

    ev->Get()->UpdateCompleted.SetValue(EPersistResult::Cancelled);
}

void TPartitionActor::HandleSetVChunkTouchedDuringDelete(
    const TEvPartitionDirectPrivate::TEvSetVChunkTouched::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    YDB_LOG_INFO_CTX(ctx, "Drop SetVChunkTouched during delete: vchunk",
        {"withTime", LogTitle.GetWithTime()},
        {"VChunkIndex", msg->VChunkIndex});

    msg->UpdateCompleted.SetValue(EPersistResult::Cancelled);
}

// Ignore fast path service shutdown during delete
void TPartitionActor::HandleFastPathServiceShutdownDuringDelete(
    const TEvPartitionDirectPrivate::TEvFastPathServiceShutdown::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "FastPathServiceShutdown during delete",
        {"withTime", LogTitle.GetWithTime()});

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

    YDB_LOG_INFO_CTX(ctx, "Drop AddHost during delete FastPathService stopped",
        {"withTime", LogTitle.GetWithTime()},
        {"dbgId", dbgId});
}

void TPartitionActor::HandlePersistHostHealthDuringDelete(
    const TEvPartitionDirectPrivate::TEvPersistHostHealth::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    YDB_LOG_INFO_CTX(ctx, "Drop PersistHostHealth during delete",
        {"withTime", LogTitle.GetWithTime()});
}

void TPartitionActor::HandleRemoveHostFromDBGDuringDelete(
    const TEvPartitionDirectPrivate::TEvRemoveHostFromDBG::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const size_t dbgId = ev->Get()->DirectBlockGroupId;
    const size_t hostIndex = ev->Get()->HostIndex;
    if (FastPathService) {
        RejectRemoveHost(ctx, dbgId, hostIndex, "partition is being deleted");
        return;
    }

    YDB_LOG_INFO_CTX(ctx, "Drop RemoveHost during delete FastPathService stopped",
        {"withTime", LogTitle.GetWithTime()},
        {"dbgId", dbgId});
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateDelete)
{
    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Processing",
        {"withTime", LogTitle.GetWithTime()},
        {"event", ev->GetTypeName().data()},
        {"sender", ev->Sender.LocalId()});

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
        HFunc(
            NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeRequest,
            HandleStatVolumeDuringDelete);
        // Ignore update vchunk config during delete
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateVChunkConfig,
            HandleUpdateVChunkConfigDuringDelete);
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateDirtyMapState,
            HandleUpdateDirtyMapStateDuringDelete);
        HFunc(
            TEvPartitionDirectPrivate::TEvSetVChunkTouched,
            HandleSetVChunkTouchedDuringDelete);
        // Ignore fast path service shutdown during delete
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceShutdown,
            HandleFastPathServiceShutdownDuringDelete);
        // Ignore add host to dbg during delete
        HFunc(
            TEvPartitionDirectPrivate::TEvAddHostToDBG,
            HandleAddHostToDBGDuringDelete);
        HFunc(
            TEvPartitionDirectPrivate::TEvPersistHostHealth,
            HandlePersistHostHealthDuringDelete);
        HFunc(
            TEvPartitionDirectPrivate::TEvRemoveHostFromDBG,
            HandleRemoveHostFromDBGDuringDelete);
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
