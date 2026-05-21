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
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <util/system/fs.h>

#include <unistd.h>

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
        "%s TPartitionActor: initialization started",
        LogTitle.GetWithTime().c_str());
}

TPartitionActor::~TPartitionActor() = default;

void TPartitionActor::PassAway()
{
    LOG_INFO(
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "TPartitionActor: before detach");
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
            TabletID(),
            Executor()->Generation(),   // generation
            i,                          // direct block group index
            std::move(ddiskIds),
            std::move(persistentBufferDDiskIds));

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
    TVector<TVChunkConfig> vChunkConfigs,
    THashMap<ui32, ui64> barrierLsns)
{
    LogTitle.SetDiskId(VolumeConfig.GetDiskId());
    LogTitle.SetGeneration(Executor()->Generation());

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Starting",
        LogTitle.GetWithTime().c_str());

    auto nbsService = GetNbsService();
    Y_ABORT_UNLESS(nbsService);
    Y_ABORT_UNLESS(nbsService->Scheduler);
    Y_ABORT_UNLESS(nbsService->Timer);

    TVChunkConfigByIndex vChunkConfigsByIndex;
    vChunkConfigsByIndex.reserve(vChunkConfigs.size());
    for (const auto& cfg: vChunkConfigs) {
        vChunkConfigsByIndex[cfg.VChunkIndex] = cfg;
    }

    BarrierLsns = std::move(barrierLsns);

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

    FastPathService->Run();

    LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, FastPathService);

    {
        auto service = GetNbsService();

        TString socketPath = "/tmp/" + VolumeConfig.GetDiskId() + ".sock";
        NVhost::TStorageOptions options{
            .DiskId = VolumeConfig.GetDiskId(),
            .ClientId = "client-1",
            .BlockSize = VolumeConfig.GetBlockSize(),
            .StripeSize = StorageConfig->GetStripeSize(),
            .BlocksCount = blockCount,
            .VChunkSize = StorageConfig->GetVChunkSize(),
            .VhostQueuesCount = 1};
        service->VhostServer->StartEndpoint(
            std::move(socketPath),
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

    ScheduleBarrierCleanup(ctx);
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        LogTitle.GetWithTime().c_str(),
        msg->Record.DebugString().data());

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
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "%s HandleControllerAllocateDDiskBlockGroupResult finished with "
            "error: %d, reason: %s",
            LogTitle.GetWithTime().c_str(),
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
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

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle UpdateVolumeConfig request. Version: %d",
        LogTitle.GetWithTime().c_str(),
        msg->Record.GetVolumeConfig().GetVersion());

    if (DdiskBlockGroupAllocated) {
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
    auto& cfg = ev->Get()->VChunkConfig;

    LOG_DEBUG_S(
        ctx,
        NKikimrServices::NBS_PARTITION,
        LogTitle.GetWithTime().c_str()
            << " Handle UpdateVChunkConfig, vChunkIndex: " << cfg.VChunkIndex);

    ExecuteTx(ctx, CreateTx<TUpdateVChunkConfig>(std::move(cfg)));
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::ScheduleBarrierCleanup(const TActorContext& ctx)
{
    const auto interval = StorageConfig->GetBarrierCleanupInterval();
    if (interval == TDuration::Zero()) {
        return;
    }
    if (!FastPathService) {
        return;
    }
    if (FastPathService->GetDirectBlockGroups().empty()) {
        return;
    }
    if (BarrierCleanupScheduled) {
        return;
    }
    BarrierCleanupScheduled = true;
    ctx.Schedule(
        interval,
        new TEvPartitionDirectPrivate::TEvBarrierCleanupWakeup{});
}

void TPartitionActor::HandleBarrierCleanupWakeup(
    const TEvPartitionDirectPrivate::TEvBarrierCleanupWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    BarrierCleanupScheduled = false;

    if (BarrierCleanupInFlight) {
        // Previous cycle is still working — try again later.
        ScheduleBarrierCleanup(ctx);
        return;
    }

    StartBarrierCleanupCycle(ctx);
}

void TPartitionActor::StartBarrierCleanupCycle(const TActorContext& ctx)
{
    if (!FastPathService) {
        ScheduleBarrierCleanup(ctx);
        return;
    }

    const auto& directBlockGroups = FastPathService->GetDirectBlockGroups();
    if (directBlockGroups.empty()) {
        ScheduleBarrierCleanup(ctx);
        return;
    }

    BarrierCleanupInFlight = true;

    TVector<NThreading::TFuture<ui64>> futures;
    TVector<size_t> dbgIndexes;
    futures.reserve(directBlockGroups.size());
    dbgIndexes.reserve(directBlockGroups.size());
    for (const auto& dbg: directBlockGroups) {
        dbgIndexes.push_back(dbg->GetDirectBlockGroupIndex());
        futures.push_back(dbg->ComputeBarrierLsnAsync());
    }

    auto combined = NThreading::WaitAll(futures);
    combined.Subscribe(
        [selfId = SelfId(),
         actorSystem = ctx.ActorSystem(),
         futures = std::move(futures),
         dbgIndexes = std::move(dbgIndexes)](const auto&) mutable
        {
            THashMap<ui32, ui64> perDbgLsn;
            perDbgLsn.reserve(futures.size());
            for (size_t i = 0; i < futures.size(); ++i) {
                perDbgLsn[static_cast<ui32>(dbgIndexes[i])] =
                    futures[i].GetValue();
            }
            actorSystem->Send(
                selfId,
                new TEvPartitionDirectPrivate::TEvBarrierLsnsReady{
                    std::move(perDbgLsn)});
        });
}

void TPartitionActor::HandleBarrierLsnsReady(
    const TEvPartitionDirectPrivate::TEvBarrierLsnsReady::TPtr& ev,
    const TActorContext& ctx)
{
    auto& perDbgLsn = ev->Get()->PerDbgLsn;

    // Filter out DBGs whose barrier wouldn't advance (LSN == 0 or unchanged
    // relative to what's already persisted).
    THashMap<ui32, ui64> toPersist;
    for (const auto& [dbgIndex, lsn]: perDbgLsn) {
        if (lsn == 0) {
            continue;
        }
        const auto it = BarrierLsns.find(dbgIndex);
        if (it != BarrierLsns.end() && it->second >= lsn) {
            continue;
        }
        toPersist.emplace(dbgIndex, lsn);
    }

    if (toPersist.empty()) {
        BarrierCleanupInFlight = false;
        ScheduleBarrierCleanup(ctx);
        return;
    }

    ExecuteTx(ctx, CreateTx<TStoreBarrierLsns>(std::move(toPersist)));
}

void TPartitionActor::OnBarrierLsnsPersisted(
    const TActorContext& ctx,
    THashMap<ui32, ui64> perDbgLsn)
{
    for (const auto& [dbgIndex, lsn]: perDbgLsn) {
        BarrierLsns[dbgIndex] = lsn;
    }

    TVector<NThreading::TFuture<void>> futures;
    futures.reserve(perDbgLsn.size());
    if (FastPathService) {
        for (const auto& dbg: FastPathService->GetDirectBlockGroups()) {
            const auto it = perDbgLsn.find(
                static_cast<ui32>(dbg->GetDirectBlockGroupIndex()));
            if (it == perDbgLsn.end()) {
                continue;
            }
            futures.push_back(dbg->IssueBarrierAsync(it->second));
        }
    }

    if (futures.empty()) {
        BarrierCleanupInFlight = false;
        ScheduleBarrierCleanup(ctx);
        return;
    }

    auto combined = NThreading::WaitAll(futures);
    combined.Subscribe(
        [selfId = SelfId(),
         actorSystem = ctx.ActorSystem()](const auto&) mutable
        {
            actorSystem->Send(
                selfId,
                new TEvPartitionDirectPrivate::TEvBarrierCycleDone{});
        });
}

void TPartitionActor::HandleBarrierCycleDone(
    const TEvPartitionDirectPrivate::TEvBarrierCycleDone::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    BarrierCleanupInFlight = false;
    ScheduleBarrierCleanup(ctx);
}

///////////////////////////////////////////////////////////////////////////////

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
            TEvPartitionDirectPrivate::TEvBarrierCleanupWakeup,
            HandleBarrierCleanupWakeup);
        HFunc(
            TEvPartitionDirectPrivate::TEvBarrierLsnsReady,
            HandleBarrierLsnsReady);
        HFunc(
            TEvPartitionDirectPrivate::TEvBarrierCycleDone,
            HandleBarrierCycleDone);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG_S(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::NBS_PARTITION,
                    "Unhandled event type: " << ev->GetTypeRewrite()
                                             << " event: " << ev->ToString());
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
