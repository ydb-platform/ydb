#include "partition_direct_actor.h"

#include "direct_block_group_impl.h"
#include "fast_path_service.h"
#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/counters_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/core/mon.h>

#include <util/system/fs.h>

#include <unistd.h>

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NActors;

TPartitionActor::TPartitionActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletBase<TPartitionActor>(
          tablet,
          NKikimr::TTabletStorageInfoPtr(info),
          nullptr)
    , LogTitle{GetCycleCount(), TLogTitle::TPartitionDirect{.TabletId = TabletID()}}
    , StorageConfig(GetNbsService()->StorageConfig)
{
    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s initialization started",
        LogTitle.GetWithTime().c_str());
}

TPartitionActor::~TPartitionActor() = default;

void TPartitionActor::OnDetach(const TActorContext& ctx)
{
    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s OnDetach",
        LogTitle.GetWithTime().c_str());

    DetachEndpointAddDie(ctx);
}

void TPartitionActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s OnTabletDead %s",
        LogTitle.GetWithTime().c_str(),
        TEvTablet::TEvTabletDead::Str(msg->Reason));

    DetachEndpointAddDie(ctx);
}

// Tablet received poison pill, cleanup resources
void TPartitionActor::PassAway()
{
    const auto& ctx = NActors::TActivationContext::AsActorContext();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s PassAway",
        LogTitle.GetWithTime().c_str());

    // Do not call Die() here: Die() invokes PassAway() again.
    CleanupResources(ctx);
    TActor::PassAway();
}

void TPartitionActor::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Started NBS partition: actor id %s",
        LogTitle.GetWithTime().c_str(),
        SelfId().ToString().data());

    if (!Executor()->GetStats().IsFollower()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Executing InitSchema transaction",
            LogTitle.GetWithTime().c_str());
        ExecuteTx(ctx, CreateTx<TInitSchema>());
    }

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TPartitionActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
}

void TPartitionActor::CleanupResources(const TActorContext& ctx)
{
    if (LoadActorAdapter) {
        ctx.Send(LoadActorAdapter, new TEvents::TEvPoisonPill());
        LoadActorAdapter = {};
    }

    if (CleanupActor) {
        ctx.Send(CleanupActor, new TEvents::TEvPoisonPill());
        CleanupActor = {};
    }

    NTabletPipe::CloseAndForgetClient(SelfId(), BSControllerPipeClient);
    if (AddHostInFlight) {
        NTabletPipe::CloseAndForgetClient(
            SelfId(),
            AddHostInFlight->BSPipeClient);
        AddHostInFlight.reset();
    }

    GetNbsService()->VhostServer->DetachStorage(GetSocketPath());

    // It is assumed that the transaction to the local database is always
    // successful. If the Tablet finishes its work, then it is necessary to
    // respond to all pending requests so that there are no leakage resources.
    // We will do this after the initiator of the request is stopped.
    auto failUpdateRequests =
        [executingConfigPromises =
             std::move(ExecutingUpdateVChunkConfigPromises),
         pendingConfigRequests = std::move(PendingUpdateVChunkConfigRequests),
         executingDirtyMapPromises =
             std::move(ExecutingUpdateDirtyMapStatePromises),
         pendingDirtyMapRequests =
             std::move(PendingUpdateDirtyMapStateRequests)]() mutable
    {
        for (auto& promise: executingConfigPromises) {
            promise.TrySetValue(EPersistResult::Cancelled);
        }
        for (auto& req: pendingConfigRequests) {
            req.UpdateCompleted.TrySetValue(EPersistResult::Cancelled);
        }

        for (auto& promise: executingDirtyMapPromises) {
            promise.TrySetValue(EPersistResult::Cancelled);
        }
        for (auto& req: pendingDirtyMapRequests) {
            req.UpdateCompleted.TrySetValue(EPersistResult::Cancelled);
        }
    };

    if (FastPathService) {
        auto onStop = FastPathService->Stop();
        onStop.Subscribe(
            [failUpdateRequests = std::move(failUpdateRequests)](
                const NThreading::TFuture<void>& stopFuture) mutable
            {
                Y_UNUSED(stopFuture);
                failUpdateRequests();
            });
        FastPathService.reset();
    } else {
        failUpdateRequests();
    }
}

void TPartitionActor::DetachEndpointAddDie(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s DetachEndpointAddDie",
        LogTitle.GetWithTime().c_str());

    CleanupResources(ctx);
    Die(ctx);
}

void TPartitionActor::ReportTabletState(const TActorContext& ctx)
{
    auto service =
        NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<
        NNodeWhiteboard::TEvWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        STATE_WORK);

    NYdb::NBS::Send(ctx, service, std::move(request));
}

void TPartitionActor::HandleConnect(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Client %s %s connected to volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Client %s %s destroyed",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Pipe client %s server %s connected to volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Pipe client %s server %s disconnected from volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TPartitionActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Pipe client %s server %s got destroyed for volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev)
{
    StateInitImpl(ev, SelfId());
}

TFastPathServicePtr TPartitionActor::CreateFastPathService(
    const TVChunkConfigs& vChunkConfigs,
    const TDirtyMapStateProtos& dirtyMapStates)
{
    const auto nbsService = GetNbsService();
    Y_ABORT_UNLESS(nbsService);
    Y_ABORT_UNLESS(nbsService->Scheduler);
    Y_ABORT_UNLESS(nbsService->Timer);

    TVector<IDirectBlockGroupPtr> directBlockGroups;
    directBlockGroups.reserve(DirectBlockGroupsCount);
    TVector<NTransport::IChaosInjectorControlPtr> chaosInjectorControls;
    chaosInjectorControls.reserve(DirectBlockGroupsCount);

    auto executors =
        nbsService->ExecutorPool.GetExecutors(DirectBlockGroupsCount);

    // Session counters are aggregated at the disk level: all direct block
    // groups of this tablet share the same counters chain, so per-group
    // increments naturally sum up into disk-level counters.
    NMonitoring::TDynamicCounterPtr dbgCountersRoot = MakeCountersChain(
        AppData()->Counters,
        StorageConfig->GetDDiskPoolName(),
        DiskDescription);

    for (ui32 dbgIndex = 0; dbgIndex < DirectBlockGroupsCount; dbgIndex++) {
        const auto& conn =
            DirectBlockGroupsConnections.GetDirectBlockGroupConnections(
                dbgIndex);
        TVector<NBsController::TDDiskId> ddiskIds;
        for (const auto& connection: conn.GetConnections()) {
            ddiskIds.push_back(
                NBsController::TDDiskId(connection.GetDDiskId()));
        }
        TVector<NBsController::TDDiskId> persistentBufferDDiskIds;
        for (const auto& connection: conn.GetConnections()) {
            persistentBufferDDiskIds.push_back(NBsController::TDDiskId(
                connection.GetPersistentBufferDDiskId()));
        }

        const bool enableChecksums =
            nbsService->StorageConfig->GetEnableChecksums();
        auto transport = NTransport::CreateStorageTransport(
            TActivationContext::ActorSystem(),
            DiskDescription,
            dbgIndex,
            nbsService->StorageConfig->GetUseDirectSessionTransport(),
            enableChecksums);

        // TODO: Create the wrapper only when chaos injection is enabled and
        // keep a null control for this DBG otherwise.
        auto chaosInjector =
            NTransport::CreateTransportChaosInjector(std::move(transport));
        chaosInjectorControls.emplace_back(chaosInjector);
        transport = std::move(chaosInjector);

        auto directBlockGroup = std::make_shared<TDirectBlockGroup>(
            TActivationContext::ActorSystem(),
            nbsService->StorageConfig,
            executors[dbgIndex],
            DiskDescription,
            dbgIndex,
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds),
            std::move(transport),
            dbgCountersRoot);

        directBlockGroups.emplace_back(std::move(directBlockGroup));
    }

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    return std::make_shared<TFastPathService>(
        TActivationContext::ActorSystem(),
        SelfId(),
        DiskDescription,
        blockCount,
        VolumeConfig.GetBlockSize(),
        std::move(directBlockGroups),
        std::move(chaosInjectorControls),
        vChunkConfigs,
        dirtyMapStates,
        StorageConfig,
        nbsService->Scheduler,
        nbsService->Timer,
        AppData()->Counters);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::CreateBSControllerPipeClient(
    const NActors::TActorContext& ctx)
{
    BSControllerPipeClient = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
}

void TPartitionActor::AllocateDDiskBlockGroup(const NActors::TActorContext& ctx)
{
    CreateBSControllerPipeClient(ctx);

    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig->GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig->GetPersistentBufferDDiskPoolName());

    // TODO: fill with tablet id
    request->Record.SetTabletId(TabletID());

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    const ui64 regionsCount =
        AlignUp(blockCount * VolumeConfig.GetBlockSize(), RegionSize) /
        RegionSize;

    for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
        auto* query = request->Record.AddQueries();
        query->SetDirectBlockGroupId(i);
        query->SetTargetNumVChunks(regionsCount);
    }

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

TString TPartitionActor::GetSocketPath() const
{
    return "/tmp/" + VolumeConfig.GetDiskId() + ".sock";
}

void TPartitionActor::Start(
    const NActors::TActorContext& ctx,
    TDirectBlockGroupsConnections directBlockGroupsConnections,
    const TVChunkConfigs& vChunkConfigs,
    const TDirtyMapStateProtos& dirtyMapStates)
{
    LogTitle.SetDiskId(VolumeConfig.GetDiskId());
    LogTitle.SetGeneration(Executor()->Generation());
    DiskDescription.DiskId = VolumeConfig.GetDiskId();
    DiskDescription.TabletId = TabletID();
    DiskDescription.Generation = Executor()->Generation();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Starting",
        LogTitle.GetWithTime().c_str());

    DirectBlockGroupsConnections = std::move(directBlockGroupsConnections);

    FastPathService = CreateFastPathService(vChunkConfigs, dirtyMapStates);

    // Synchronous start mode - requests pass as the initial quorum of Locked
    // DDisk sessions across all DBGs is achieved.
    // TODO: make optional via StorageConfig after implementation of async mode.
    FastPathService->Run().Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = SelfId()]   //
        (const NThreading::TFuture<void>&) mutable
        {
            // This callback runs OUTSIDE the actor thread - on the DBG's
            // executor-thread
            auto event = std::make_unique<
                TEvPartitionDirectPrivate::TEvFastPathServiceReady>();
            actorSystem->Send(selfId, event.release());
        });
}

void TPartitionActor::HandleFastPathServiceReady(
    const TEvPartitionDirectPrivate::TEvFastPathServiceReady::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s All DBGs reached initial locked quorum, opening endpoint",
        LogTitle.GetWithTime().c_str());

    // Re-send the BSC request for an add-host in flight at the last restart
    // (no live add can be in flight this early). BSController is idempotent.
    if (AddHostInFlight.has_value()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Replaying in-flight AddHost dbgId=%lu newHostIndex=%s",
            LogTitle.GetWithTime().c_str(),
            AddHostInFlight->DirectBlockGroupId,
            PrintHostIndex(AddHostInFlight->NewHostIndex).c_str());
        SendAllocateDDiskForAddHost(
            ctx,
            AddHostInFlight->DirectBlockGroupId,
            AddHostInFlight->NewHostIndex);
    }

    LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, FastPathService);

    {
        auto service = GetNbsService();

        const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
        NVhost::TStorageOptions options{
            .DiskId = VolumeConfig.GetDiskId(),
            .ClientId = "client-1",
            .BlockSize = VolumeConfig.GetBlockSize(),
            .StripeSize = StorageConfig->GetStripeSize(),
            .BlocksCount = blockCount,
            .VChunkSize = StorageConfig->GetVChunkSize(),
            .VhostQueuesCount = StorageConfig->GetVhostQueuesCount(),
            .Generation = Executor()->Generation()};
        service->VhostServer->StartEndpoint(
            GetSocketPath(),
            FastPathService,
            FastPathService,
            options);
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Started NBS LoadActorAdapter: %s",
        LogTitle.GetWithTime().c_str(),
        LoadActorAdapter.ToString().c_str());
}

void TPartitionActor::HandleFastPathServiceShutdown(
    const TEvPartitionDirectPrivate::TEvFastPathServiceShutdown::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!FastPathService) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s FastPathService is not started",
            LogTitle.GetWithTime().c_str());
        Send(
            ctx.SelfID,
            std::make_unique<
                TEvPartitionDirectPrivate::TEvFastPathServiceStopped>(),
            0,   //   flags
            ev->Cookie);

        Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvPartitionDirectPrivate::TEvFastPathServiceStopped>());

        return;
    }

    auto onStop = FastPathService->Stop();
    onStop.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = ctx.SelfID,
         recipient = ev->Sender,
         cookie = ev->Cookie]   //
        (const NThreading::TFuture<void>& f)
        {
            Y_UNUSED(f);
            {
                auto event = std::make_unique<
                    TEvPartitionDirectPrivate::TEvFastPathServiceStopped>();
                actorSystem->Send(
                    selfId,
                    event.release(),
                    0,   // flags
                    cookie);
            }
            {
                auto event = std::make_unique<
                    TEvPartitionDirectPrivate::TEvFastPathServiceStopped>();
                actorSystem->Send(
                    recipient,
                    event.release(),
                    0,   // flags
                    cookie);
            }
        });
}

void TPartitionActor::HandleFastPathServiceStopped(
    const TEvPartitionDirectPrivate::TEvFastPathServiceStopped::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s FastPathService stopped",
        LogTitle.GetWithTime().c_str());
}

void TPartitionActor::HandlePoisonByBlockedGeneration(
    const TEvPartitionDirectPrivate::TEvPoison::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_CRIT(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s SUICIDE by BLOCKED generation. Reason: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Reason.c_str());

    ctx.Send(Tablet(), std::make_unique<TEvents::TEvPoisonPill>().release());
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        LogTitle.GetWithTime().c_str(),
        ev->Get()->Record.DebugString().data());

    // The first allocation response sets up the group; any later one is the
    // result of an add-host request.
    if (DDiskBlockGroupAllocated) {
        HandleAddHostAllocationResult(ev, ctx);
    } else {
        HandleInitialAllocationResult(ev, ctx);
    }
}

void TPartitionActor::HandleInitialAllocationResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        Y_ABORT_UNLESS(
            msg->Record.GetResponses().size() == DirectBlockGroupsCount);

        TDirectBlockGroupsConnections ids;
        for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
            auto* directBlockGroupConnections =
                ids.AddDirectBlockGroupConnections();
            const auto& response = msg->Record.GetResponses()[i];
            for (const auto& node: response.GetNodes()) {
                auto* connection =
                    directBlockGroupConnections->AddConnections();
                connection->MutableDDiskId()->CopyFrom(node.GetDDiskId());
                connection->MutablePersistentBufferDDiskId()->CopyFrom(
                    node.GetPersistentBufferDDiskId());
            }
        }

        DDiskBlockGroupAllocated = true;
        ExecuteTx(ctx, CreateTx<TStorePartitionIds>(std::move(ids)));
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleControllerAllocateDDiskBlockGroupResult finished with "
            "error: %d, reason: %s",
            LogTitle.GetWithTime().c_str(),
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }

    NTabletPipe::CloseAndForgetClient(SelfId(), BSControllerPipeClient);
}

void TPartitionActor::HandleGetLoadActorAdapterActorId(
    const TEvService::TEvGetLoadActorAdapterActorIdRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvGetLoadActorAdapterActorIdResponse>();
    response->Record.SetActorId(LoadActorAdapter.ToString());
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request. Version: %d",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetVolumeConfig().GetVersion());

    if (DDiskBlockGroupAllocated) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Already has ddisk connections",
            LogTitle.GetWithTime().c_str());

        auto response = std::make_unique<
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
        response->Record.SetStatus(NKikimrBlockStore::ERROR);
        ctx.Send(ev->Sender, response.release());
        return;
    }

    const auto& volumeConfig = msg->Record.GetVolumeConfig();
    Y_ABORT_UNLESS(volumeConfig.PartitionsSize() == 1);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request VolumeConfig: %s",
        LogTitle.GetWithTime().c_str(),
        volumeConfig.DebugString().c_str());

    ExecuteTx(ctx, CreateTx<TStoreVolumeConfig>(volumeConfig));

    // Send response back to volume
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(msg->Record.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrBlockStore::OK);

    LOG_INFO(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Sending UpdateVolumeConfig response OK",
        LogTitle.GetWithTime().c_str());

    ctx.Send(ev->Sender, response.release());
}

void TPartitionActor::HandleUpdateVChunkConfig(
    const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVChunkConfig %s %s",
        LogTitle.GetWithTime().c_str(),
        msg->VChunkConfig.DebugPrint().c_str(),
        ExecutingUpdateVChunkConfig ? "later" : "now");

    if (ExecutingUpdateVChunkConfig) {
        PendingUpdateVChunkConfigRequests.push_back(
            {.VChunkConfig = std::move(msg->VChunkConfig),
             .UpdateCompleted = std::move(msg->UpdateCompleted)});
    } else {
        Y_DEBUG_ABORT_UNLESS(PendingUpdateVChunkConfigRequests.empty());

        ExecutingUpdateVChunkConfig = true;
        ExecuteTx(
            ctx,
            CreateTx<TUpdateVChunkConfig>(
                TTxPartition::TUpdateVChunkConfig::TUpdateConfigRequests{
                    {.VChunkConfig = std::move(msg->VChunkConfig),
                     .UpdateCompleted = std::move(msg->UpdateCompleted)}}));
    }
}

void TPartitionActor::HandleUpdateDirtyMapState(
    const TEvPartitionDirectPrivate::TEvUpdateDirtyMapState::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateDirtyMapState vchunk %u %s",
        LogTitle.GetWithTime().c_str(),
        msg->VChunkIndex,
        ExecutingUpdateDirtyMapState ? "later" : "now");

    if (ExecutingUpdateDirtyMapState) {
        PendingUpdateDirtyMapStateRequests.push_back(
            {.VChunkIndex = msg->VChunkIndex,
             .State = std::move(msg->State),
             .UpdateCompleted = std::move(msg->UpdateCompleted)});
    } else {
        Y_DEBUG_ABORT_UNLESS(PendingUpdateDirtyMapStateRequests.empty());

        ExecutingUpdateDirtyMapState = true;
        ExecuteTx(
            ctx,
            CreateTx<TUpdateDirtyMapState>(
                TTxPartition::TUpdateDirtyMapState::TUpdateStateRequests{
                    {.VChunkIndex = msg->VChunkIndex,
                     .State = std::move(msg->State),
                     .UpdateCompleted = std::move(msg->UpdateCompleted)}}));
    }
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleCommonEvents(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);
        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvTabletPipe::TEvServerDestroyed, HandleServerDestroyed);
        HFunc(
            TEvService::TEvGetLoadActorAdapterActorIdRequest,
            HandleGetLoadActorAdapterActorId);
        HFunc(
            TEvPartitionDirectPrivate::TEvPoison,
            HandlePoisonByBlockedGeneration);
        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_ERROR(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "%s Unhandled event type: %u event %s ",
                    LogTitle.GetWithTime().c_str(),
                    ev->GetTypeRewrite(),
                    ev->ToString().c_str());
            }
            break;
    }
}

STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "%s Processing event: %s from sender: %lu",
        LogTitle.GetWithTime().c_str(),
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
            HandleControllerAllocateDDiskBlockGroupResult);
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateVChunkConfig,
            HandleUpdateVChunkConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateDirtyMapState,
            HandleUpdateDirtyMapState);
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceReady,
            HandleFastPathServiceReady);
        HFunc(TEvPartitionDirectPrivate::TEvAddHostToDBG, HandleAddHostToDBG);

        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceShutdown,
            HandleFastPathServiceShutdown);

        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceStopped,
            HandleFastPathServiceStopped);

        HFunc(TEvService::TEvDeletePartitionRequest, HandleDeletePartition);

        default:
            HandleCommonEvents(ev);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
