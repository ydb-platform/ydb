#include "partition_direct_actor.h"

#include "direct_block_group_impl.h"
#include "fast_path_service.h"
#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/partition_direct.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/core/mon.h>

#include <util/system/fs.h>

#include <unistd.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

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
    YDB_LOG_INFO_CTX(NActors::TActivationContext::AsActorContext(), "TPartitionActor: initialization started",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});
}

TPartitionActor::~TPartitionActor() = default;

void TPartitionActor::PassAway()
{
    YDB_LOG_INFO_CTX(NActors::TActivationContext::AsActorContext(), "TPartitionActor: before detach");
}

void TPartitionActor::OnDetach(const TActorContext& ctx)
{
    Die(ctx);
}

void TPartitionActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

void TPartitionActor::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    YDB_LOG_INFO_CTX(ctx, "Started NBS partition: actor id",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_SelfId", SelfId()});

    if (!Executor()->GetStats().IsFollower()) {
        YDB_LOG_INFO_CTX(ctx, "Executing InitSchema transaction",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});
        ExecuteTx(ctx, CreateTx<TInitSchema>());
    }

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TPartitionActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
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

void TPartitionActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_DEBUG_CTX(ctx, "Pipe client server connected to volume",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

void TPartitionActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_DEBUG_CTX(ctx, "Pipe client server disconnected from volume",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

void TPartitionActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_INFO_CTX(ctx, "Pipe client server got destroyed for volume",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev)
{
    StateInitImpl(ev, SelfId());
}

TVector<IDirectBlockGroupPtr> TPartitionActor::CreateDirectBlockGroups(
    TDirectBlockGroupsConnections directBlockGroupsConnections)
{
    const auto nbsService = GetNbsService();
    TVector<IDirectBlockGroupPtr> directBlockGroups;
    auto executors =
        nbsService->ExecutorPool.GetExecutors(DirectBlockGroupsCount);

    for (size_t i = 0; i < DirectBlockGroupsCount; i++) {
        const auto& conn =
            directBlockGroupsConnections.GetDirectBlockGroupConnections(i);
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

        auto directBlockGroup = std::make_shared<TDirectBlockGroup>(
            TActivationContext::ActorSystem(),
            nbsService->StorageConfig,
            executors[i],
            VolumeConfig.GetDiskId(),
            TabletID(),
            Executor()->Generation(),   // generation
            i,                          // direct block group index
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds),
            std::make_unique<NTransport::TICStorageTransport>(
                TActivationContext::ActorSystem()));

        directBlockGroups.emplace_back(std::move(directBlockGroup));
    }

    return directBlockGroups;
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

void TPartitionActor::Start(
    const NActors::TActorContext& ctx,
    TDirectBlockGroupsConnections directBlockGroupsConnections,
    TVector<TVChunkConfig> vChunkConfigs)
{
    LogTitle.SetDiskId(VolumeConfig.GetDiskId());
    LogTitle.SetGeneration(Executor()->Generation());

    YDB_LOG_INFO_CTX(ctx, "Starting",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

    auto nbsService = GetNbsService();
    Y_ABORT_UNLESS(nbsService);
    Y_ABORT_UNLESS(nbsService->Scheduler);
    Y_ABORT_UNLESS(nbsService->Timer);

    TVChunkConfigByIndex vChunkConfigsByIndex;
    vChunkConfigsByIndex.reserve(vChunkConfigs.size());
    for (const auto& cfg: vChunkConfigs) {
        vChunkConfigsByIndex[cfg.GetVChunkIndex()] = cfg;
    }

    const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
    FastPathService = std::make_shared<TFastPathService>(
        TActivationContext::ActorSystem(),
        SelfId(),
        TabletID(),
        VolumeConfig.GetDiskId(),
        blockCount,
        VolumeConfig.GetBlockSize(),
        CreateDirectBlockGroups(std::move(directBlockGroupsConnections)),
        std::move(vChunkConfigsByIndex),
        StorageConfig,
        nbsService->Scheduler,
        nbsService->Timer,
        AppData()->Counters);

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
    YDB_LOG_INFO_CTX(ctx, "All DBGs reached initial locked quorum, opening endpoint",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

    LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, FastPathService);

    {
        auto service = GetNbsService();

        const ui64 blockCount = VolumeConfig.GetPartitions(0).GetBlockCount();
        TString socketPath = "/tmp/" + VolumeConfig.GetDiskId() + ".sock";
        NVhost::TStorageOptions options{
            .DiskId = VolumeConfig.GetDiskId(),
            .ClientId = "client-1",
            .BlockSize = VolumeConfig.GetBlockSize(),
            .StripeSize = StorageConfig->GetStripeSize(),
            .BlocksCount = blockCount,
            .VChunkSize = StorageConfig->GetVChunkSize(),
            .VhostQueuesCount = StorageConfig->GetVhostQueuesCount()};
        service->VhostServer->StartEndpoint(
            std::move(socketPath),
            FastPathService,
            FastPathService,
            options);
    }

    YDB_LOG_INFO_CTX(ctx, "Started NBS",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"loadActorAdapter", LoadActorAdapter});
}

void TPartitionActor::HandleFastPathServiceShutdown(
    const TEvPartitionDirectPrivate::TEvFastPathServiceShutdown::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!FastPathService) {
        YDB_LOG_INFO_CTX(ctx, "FastPathService is not started",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});
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

    YDB_LOG_INFO_CTX(ctx, "FastPathService stopped",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_INFO_CTX(ctx, "HandleControllerAllocateDDiskBlockGroupResult record",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"is", msg->Record.DebugString().data()});

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

        DdiskBlockGroupAllocated = true;
        ExecuteTx(ctx, CreateTx<TStorePartitionIds>(std::move(ids)));
    } else {
        YDB_LOG_ERROR_CTX(ctx, "HandleControllerAllocateDDiskBlockGroupResult finished with",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"error", msg->Record.GetStatus()},
            {"reason", msg->Record.GetErrorReason().data()});
    }

    NTabletPipe::CloseClient(ctx, BSControllerPipeClient);
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

    YDB_LOG_INFO_CTX(ctx, "Handle UpdateVolumeConfig request",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"version", msg->Record.GetVolumeConfig().GetVersion()});

    if (DdiskBlockGroupAllocated) {
        YDB_LOG_ERROR_CTX(ctx, "Already has ddisk connections",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

        auto response = std::make_unique<
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
        response->Record.SetStatus(NKikimrBlockStore::ERROR);
        ctx.Send(ev->Sender, response.release());
        return;
    }

    const auto& volumeConfig = msg->Record.GetVolumeConfig();
    Y_ABORT_UNLESS(volumeConfig.PartitionsSize() == 1);

    YDB_LOG_INFO_CTX(ctx, "Handle UpdateVolumeConfig request",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"volumeConfig", volumeConfig.DebugString()});

    ExecuteTx(ctx, CreateTx<TStoreVolumeConfig>(volumeConfig));

    // Send response back to volume
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(msg->Record.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrBlockStore::OK);

    YDB_LOG_INFO_CTX(TActivationContext::AsActorContext(), "Sending UpdateVolumeConfig response OK",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

    ctx.Send(ev->Sender, response.release());
}

void TPartitionActor::HandleUpdateVChunkConfig(
    const TEvPartitionDirectPrivate::TEvUpdateVChunkConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& cfg = ev->Get()->VChunkConfig;

    YDB_LOG_DEBUG_CTX(ctx, "Handle UpdateVChunkConfig",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"vChunkIndex", cfg.GetVChunkIndex()});

    ExecuteTx(ctx, CreateTx<TUpdateVChunkConfig>(std::move(cfg)));
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateWork)
{
    YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Processing",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"event", ev->GetTypeName().data()},
        {"sender", ev->Sender.LocalId()});

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
        HFunc(
            TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult,
            HandleControllerAllocateDDiskBlockGroupResult);
        HFunc(
            TEvService::TEvGetLoadActorAdapterActorIdRequest,
            HandleGetLoadActorAdapterActorId);
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvUpdateVChunkConfig,
            HandleUpdateVChunkConfig);
        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceReady,
            HandleFastPathServiceReady);

        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceShutdown,
            HandleFastPathServiceShutdown);

        HFunc(
            TEvPartitionDirectPrivate::TEvFastPathServiceStopped,
            HandleFastPathServiceStopped);

        HFunc(NMon::TEvRemoteHttpInfo, HandleHttpInfo);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                YDB_LOG_DEBUG_CTX(TActivationContext::AsActorContext(), "Unhandled event",
                    {"type", ev->GetTypeRewrite()},
                    {"event", ev->ToString()});
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
