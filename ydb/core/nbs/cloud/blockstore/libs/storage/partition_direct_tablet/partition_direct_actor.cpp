#include "partition_direct_actor.h"

#include "bsc_proxy.h"
#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>
#include <ydb/core/nbs/cloud/blockstore/libs/nbs_frontend/blockstore_facade.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/counters_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group_impl.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session_control.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/core/hfunc.h>
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

TPartitionActor::~TPartitionActor()
{
    if (!Session) {
        return;
    }
    Session->Stop();
    // Actor-system cleanup can destroy a partition without PassAway(). Its
    // blockStoreFacade registration must not retain FastPath beyond the actor
    // system.
    if (!FrontendRegistrationClosed) {
        if (auto service = GetNbsService();
            service && service->BlockStoreFacade)
        {
            service->BlockStoreFacade->UnregisterVolume(
                VolumeConfig.GetDiskId(),
                Session->GetRegistrationId());
        }
    }
}

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
    UnregisterFrontendVolume(ctx);
    if (LoadActorAdapter) {
        ctx.Send(LoadActorAdapter, new TEvents::TEvPoisonPill());
        LoadActorAdapter = {};
    }

    if (CleanupActor) {
        ctx.Send(CleanupActor, new TEvents::TEvPoisonPill());
        CleanupActor = {};
    }

    StopBscProxy(ctx);
    AddHostInFlight.reset();
    RemoveHostInFlight.reset();

    GetNbsService()->VhostServer->DetachStorage(GetSocketPath());

    // It is assumed that the transaction to the local database is always
    // successful. If the Tablet finishes its work, then it is necessary to
    // respond to all pending requests so that there are no leakage resources.
    // We will do this after the initiator of the request is stopped.
    auto failUpdateRequests =
        [executingStatePromises = std::move(ExecutingUpdateVChunkStatePromises),
         pendingStateRequests = std::move(PendingUpdateVChunkStateRequests),
         touchedVChunks = std::move(TouchedVChunks)]() mutable
    {
        for (auto& promise: executingStatePromises) {
            promise.TrySetValue(EPersistResult::Cancelled);
        }
        for (auto& req: pendingStateRequests) {
            req.UpdateCompleted.TrySetValue(EPersistResult::Cancelled);
        }
        touchedVChunks.OnSaveInterrupted();
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

void TPartitionActor::UnregisterFrontendVolume(const TActorContext& ctx)
{
    if (FrontendRegistrationClosed) {
        return;
    }
    FrontendRegistrationClosed = true;
    if (!Session) {
        return;
    }
    Session->Stop();
    if (auto& blockStoreFacade = GetNbsService()->BlockStoreFacade;
        blockStoreFacade)
    {
        blockStoreFacade->UnregisterVolume(
            VolumeConfig.GetDiskId(),
            Session->GetRegistrationId());
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Frontend unregister requested: registrationId=%s",
            LogTitle.GetWithTime().c_str(),
            Session->GetRegistrationId().c_str());
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

    const ui32 volumeDbgCount = DefaultVolumeDirectBlockGroupCount;
    TVector<IDirectBlockGroupPtr> directBlockGroups;
    auto arenaAllocator = CreateArenaAllocator();
    directBlockGroups.reserve(volumeDbgCount);
    TVector<NTransport::IChaosInjectorControlPtr> chaosInjectorControls;
    chaosInjectorControls.reserve(volumeDbgCount);

    auto executors = nbsService->ExecutorPool.GetExecutors(volumeDbgCount);

    // Session counters are aggregated at the disk level: all direct block
    // groups of this tablet share the same counters chain, so per-group
    // increments naturally sum up into disk-level counters.
    NMonitoring::TDynamicCounterPtr dbgCountersRoot = MakeCountersChain(
        AppData()->Counters,
        StorageConfig->GetDDiskPoolName(),
        DiskDescription);

    for (ui32 dbgIndex = 0; dbgIndex < volumeDbgCount; dbgIndex++) {
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
        // Temporarily preserving original behavior
        TVector<EHostHealth> hostHealths(ddiskIds.size(), EHostHealth::Online);

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
            arenaAllocator,
            TActivationContext::ActorSystem(),
            nbsService->StorageConfig,
            executors[dbgIndex],
            DiskDescription,
            VolumeConfig.GetBlockSize(),
            dbgIndex,
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds),
            std::move(hostHealths),
            conn.GetDBGConnectionsConfigGeneration(),
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
        &TouchedVChunks,
        dirtyMapStates,
        StorageConfig,
        nbsService->Scheduler,
        nbsService->Timer,
        AppData()->Counters);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::AllocateDDiskBlockGroup(const NActors::TActorContext& ctx)
{
    auto request = MakeAllocateDDiskBlockGroupRequest();

    const ui64 regionsCount = GetRegionCount(
        VolumeConfig.GetPartitions(0).GetBlockCount(),
        VolumeConfig.GetBlockSize(),
        StorageConfig->GetVChunkSize());
    const ui32 vChunkPerDbgCount = GetVChunkCountPerDirectBlockGroup(
        regionsCount,
        DefaultVolumeDirectBlockGroupCount);

    for (size_t i = 0; i < DefaultVolumeDirectBlockGroupCount; i++) {
        auto* query = request->Record.AddQueries();
        query->SetDirectBlockGroupId(i);
        query->SetTargetNumVChunks(vChunkPerDbgCount);
    }

    SendToBsc(ctx, THolder<IEventBase>(request.release()));
}

std::unique_ptr<TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>
TPartitionActor::MakeAllocateDDiskBlockGroupRequest() const
{
    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig->GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig->GetPersistentBufferDDiskPoolName());
    request->Record.SetTabletId(TabletID());
    return request;
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
    VChunkConfigs = vChunkConfigs;

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

    // Re-send the BSC request for a membership op in flight at the last
    // restart (no live op can be in flight this early). Both are idempotent.
    if (AddHostInFlight.has_value()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Replaying in-flight AddHost dbgId=%lu liveHostCount=%u",
            LogTitle.GetWithTime().c_str(),
            AddHostInFlight->DirectBlockGroupId,
            AddHostInFlight->LiveHostCount);
        SendAllocateDDiskForAddHost(ctx, AddHostInFlight->DirectBlockGroupId);
    }

    if (RemoveHostInFlight.has_value()) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Replaying in-flight RemoveHost dbgId=%lu ddisk=%s",
            LogTitle.GetWithTime().c_str(),
            RemoveHostInFlight->DirectBlockGroupId,
            RemoveHostInFlight->DDiskId.ShortDebugString().c_str());
        SendRemoveHostRequest(ctx);
    }

    LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, FastPathService);

    // MVP: use either classic gRPC or the local NBS2 vhost endpoint for a disk,
    // never both concurrently.
    if (auto& blockStoreFacade = GetNbsService()->BlockStoreFacade;
        blockStoreFacade && !FrontendRegistrationClosed)
    {
        auto sessionState = TPartitionSession::Create(
            VolumeConfig,
            FastPathService,
            FastPathService->GetVolumeConfig());
        Y_ABORT_UNLESS(
            !HasError(sessionState),
            "%s",
            FormatError(sessionState.GetError()).c_str());
        Session = sessionState.ExtractResult();
        auto registration = blockStoreFacade->RegisterVolume(
            Session,
            CreatePartitionSessionControl(ctx.ActorSystem(), SelfId()));
        Y_ABORT_UNLESS(
            !HasError(registration),
            "%s Could not publish volume: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(registration.GetError()).c_str());

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Frontend backend published: registrationId=%s "
            "blockSize=%u blocksCount=%llu",
            LogTitle.GetWithTime().c_str(),
            registration.GetResult().c_str(),
            VolumeConfig.GetBlockSize(),
            static_cast<unsigned long long>(
                VolumeConfig.GetPartitions(0).GetBlockCount()));
    }

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

    UnregisterFrontendVolume(ctx);

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
    // result of the single in-flight membership op (add xor remove).
    if (RemoveHostInFlight.has_value()) {
        HandleRemoveHostAllocationResult(ev, ctx);
    } else if (DDiskBlockGroupAllocated) {
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
        TDirectBlockGroupsConnections ids;
        for (size_t i = 0; i < VChunkPerRegionCount; i++) {
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

void TPartitionActor::ReplyUpdateVolumeConfig(
    const NActors::TActorContext& ctx,
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    NKikimrBlockStore::EStatus status)
{
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(ev->Get()->Record.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(status);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Sending UpdateVolumeConfig response %s",
        LogTitle.GetWithTime().c_str(),
        NKikimrBlockStore::EStatus_Name(status).c_str());

    ctx.Send(ev->Sender, response.release());
}

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
        // The config is already applied. SchemeShard aborts on any status
        // other than OK or ERROR_UPDATE_IN_PROGRESS. Answer a repeated
        // delivery of the applied config and a newer alter (resize) with OK.
        // Capacity is not grown yet: do not persist or reallocate, so IO
        // bounds stay at the original size until grow is implemented.
        const ui64 appliedVersion = VolumeConfig.GetVersion();
        const ui64 requestedVersion =
            msg->Record.GetVolumeConfig().GetVersion();

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Already has ddisk connections, applied version %lu, "
            "requested version %lu, status OK",
            LogTitle.GetWithTime().c_str(),
            appliedVersion,
            requestedVersion);

        ReplyUpdateVolumeConfig(ctx, ev, NKikimrBlockStore::OK);
        return;
    }

    const auto& volumeConfig = msg->Record.GetVolumeConfig();
    Y_ABORT_UNLESS(volumeConfig.PartitionsSize() == 1);

    if (!IsSupportedBlockSize(volumeConfig.GetBlockSize())) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Unsupported block size: %u",
            LogTitle.GetWithTime().c_str(),
            volumeConfig.GetBlockSize());

        ReplyUpdateVolumeConfig(ctx, ev, NKikimrBlockStore::ERROR);
        return;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request VolumeConfig: %s",
        LogTitle.GetWithTime().c_str(),
        volumeConfig.DebugString().c_str());

    ExecuteTx(ctx, CreateTx<TStoreVolumeConfig>(volumeConfig));

    ReplyUpdateVolumeConfig(ctx, ev, NKikimrBlockStore::OK);
}

void TPartitionActor::HandleMountSession(
    const TEvPartitionSession::TEvMount::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    auto* request = ev->Get();
    if (!Session || FrontendRegistrationClosed) {
        request->Result.TrySetValue(
            MakeError(E_REJECTED, "Partition registration is unavailable"));
        return;
    }
    request->Result.TrySetValue(Session->Mount(request->ClientId));
}

void TPartitionActor::HandleUnmountSession(
    const TEvPartitionSession::TEvUnmount::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    auto* request = ev->Get();
    if (!Session || FrontendRegistrationClosed) {
        request->Result.TrySetValue(
            MakeError(E_REJECTED, "Partition registration is unavailable"));
        return;
    }
    request->Result.TrySetValue(
        Session->Unmount(request->ClientId, request->SessionId));
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
        ExecutingUpdateVChunkState ? "later" : "now");

    const ui32 vChunkIndex = msg->VChunkConfig.GetVChunkIndex();
    EnqueueUpdateVChunkState(
        {.VChunkIndex = vChunkIndex,
         .VChunkConfig = std::move(msg->VChunkConfig),
         .DirtyMapState = std::move(msg->DirtyMapState),
         .UpdateCompleted = std::move(msg->UpdateCompleted)},
        ctx);
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
        ExecutingUpdateVChunkState ? "later" : "now");

    EnqueueUpdateVChunkState(
        {.VChunkIndex = msg->VChunkIndex,
         .DirtyMapState = std::move(msg->State),
         .UpdateCompleted = std::move(msg->UpdateCompleted)},
        ctx);
}

void TPartitionActor::EnqueueUpdateVChunkState(
    TTxPartition::TUpdateVChunkState::TUpdateStateRequest request,
    const NActors::TActorContext& ctx)
{
    if (ExecutingUpdateVChunkState) {
        PendingUpdateVChunkStateRequests.push_back(std::move(request));
    } else {
        Y_DEBUG_ABORT_UNLESS(PendingUpdateVChunkStateRequests.empty());

        ExecutingUpdateVChunkState = true;
        ExecuteTx(
            ctx,
            CreateTx<TUpdateVChunkState>(
                TTxPartition::TUpdateVChunkState::TUpdateStateRequests{
                    std::move(request)}));
    }
}

void TPartitionActor::HandleSetVChunkTouched(
    const TEvPartitionDirectPrivate::TEvSetVChunkTouched::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (TouchedVChunks.Add(
            ev->Get()->VChunkIndex,
            std::move(ev->Get()->UpdateCompleted)))
    {
        ExecuteTx(ctx, CreateTx<TSetVChunkTouched>(TouchedVChunks.BeginSave()));
    }
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::SendToBsc(
    const TActorContext& ctx,
    THolder<IEventBase> request,
    ui64 cookie)
{
    if (CurrentStateFunc() == &TThis::StateDelete) {
        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s Skip BSC send during delete",
            LogTitle.GetWithTime().c_str());
        return;
    }

    if (!BscProxy) {
        BscProxy = ctx.Register(new TBscProxy(SelfId(), LogTitle));
    }
    ctx.Send(BscProxy, new TBscProxy::TEvSend(std::move(request)), 0, cookie);
}

void TPartitionActor::StopBscProxy(const TActorContext& ctx)
{
    if (!BscProxy) {
        return;
    }
    ctx.Send(BscProxy, new TEvents::TEvPoisonPill());
    BscProxy = {};
}

void TPartitionActor::HandleCommonEvents(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPartitionSession::TEvMount, HandleMountSession);
        HFunc(TEvPartitionSession::TEvUnmount, HandleUnmountSession);
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
            TEvPartitionDirectPrivate::TEvSetVChunkTouched,
            HandleSetVChunkTouched);
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceReady,
            HandleFastPathServiceReady);
        HFunc(TEvPartitionDirectPrivate::TEvRenderMonPage, HandleRenderMonPage);
        HFunc(TEvPartitionDirectPrivate::TEvAddHostToDBG, HandleAddHostToDBG);
        HFunc(
            TEvPartitionDirectPrivate::TEvPersistHostHealth,
            HandlePersistHostHealth);
        HFunc(
            TEvPartitionDirectPrivate::TEvRemoveHostFromDBG,
            HandleRemoveHostFromDBG);

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

TAllocationResponse ValidateAllocationResponse(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult& msg,
    size_t dbgId,
    size_t expectedHostCount)
{
    const auto& record = msg.Record;

    if (record.GetStatus() != NKikimrProto::EReplyStatus::OK) {
        return {
            .Error = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "BSController error: " << record.GetErrorReason())};
    }
    if (record.DirectBlockGroupsSize() != 1) {
        return {
            .Error = MakeError(
                E_REJECTED,
                TStringBuilder() << "BSController returned "
                                 << record.DirectBlockGroupsSize()
                                 << " DirectBlockGroups, expected 1")};
    }

    const auto& allocated = record.GetDirectBlockGroups(0);
    if (allocated.GetDirectBlockGroupId() != dbgId) {
        return {
            .Error = MakeError(
                E_REJECTED,
                "BSController response is for a different DirectBlockGroup")};
    }
    if (allocated.GetError()) {
        return {
            .Error = MakeError(
                E_REJECTED,
                "BSController reported an error for this DirectBlockGroup")};
    }
    if (allocated.DDiskIdSize() != expectedHostCount ||
        allocated.PersistentBufferDDiskIdSize() != expectedHostCount)
    {
        return {
            .Error = MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "BSController returned " << allocated.DDiskIdSize()
                    << " ddisks / " << allocated.PersistentBufferDDiskIdSize()
                    << " pbuffers, expected " << expectedHostCount)};
    }

    return {.Group = &allocated};
}

size_t LiveHostCount(
    const ::NYdb::NBS::PartitionDirect::NProto::TDirectBlockGroupConnections&
        connections)
{
    size_t liveCount = 0;
    for (const auto& connection: connections.GetConnections()) {
        if (!connection.GetRemovedFromBSC()) {
            ++liveCount;
        }
    }
    return liveCount;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
