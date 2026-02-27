#include "partition_direct_actor.h"

#include "fast_path_service.h"
#include "load_actor_adapter.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/nbs/cloud/blockstore/bootstrap/nbs_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/region.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/mind/bscontroller/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NActors;

TPartitionActor::TPartitionActor(
        const TActorId& tablet,
        NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NKikimr::NMiniKQL::TMiniKQLFactory) {
}

void TPartitionActor::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TPartitionActor::OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TPartitionActor::OnActivateExecutor(const TActorContext& ctx) {
    Become(&TThis::StateWork);

    LOG_INFO(NActors::TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
        "Started NBS partition: actor id %s", SelfId().ToString().data());

    // Initialize StorageConfig from NBS service
    if (auto nbsService = GetNbsService()) {
        const auto& nbsConfig = nbsService->GetConfig();
        if (nbsConfig.has_nbsstorageconfig()) {
            StorageConfig.CopyFrom(nbsConfig.nbsstorageconfig());
            LOG_INFO(NActors::TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Initialized StorageConfig from NBS service config");
        }
    }

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TPartitionActor::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}

void TPartitionActor::ReportTabletState(const TActorContext& ctx)
{
    auto service = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
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
        "Pipe client %s server %s connected to volume",
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
        "Pipe client %s server %s disconnected from volume",
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
        "Pipe client %s server %s got destroyed for volume",
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

////////////////////////////////////////////////////////////////////////////////
void TPartitionActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev) {
    StateInitImpl(ev, SelfId());
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::CreateBSControllerPipeClient(const NActors::TActorContext& ctx)
{
    BSControllerPipeClient = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID()));
}

void TPartitionActor::AllocateDDiskBlockGroup(const NActors::TActorContext& ctx)
{
    CreateBSControllerPipeClient(ctx);

    auto request = std::make_unique<
        TEvBlobStorage::TEvControllerAllocateDDiskBlockGroup>();
    request->Record.SetDDiskPoolName(StorageConfig.GetDDiskPoolName());
    request->Record.SetPersistentBufferDDiskPoolName(
        StorageConfig.GetPersistentBufferDDiskPoolName());

    // TODO: fill with tablet id
    request->Record.SetTabletId(TabletID());

    for (size_t i = 0; i < 32; i++) {
        auto* query = request->Record.AddQueries();
        query->SetDirectBlockGroupId(i);

        // TODO: fill with target num v chunks. vchunk is 128MB. let us use 1 vchunk
        // since disk size will be 128MB.
        query->SetTargetNumVChunks(1);
    }

    NTabletPipe::SendData(ctx, BSControllerPipeClient, request.release());
}

void TPartitionActor::HandleControllerAllocateDDiskBlockGroupResult(
    const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "HandleControllerAllocateDDiskBlockGroupResult record is: %s",
        msg->Record.DebugString().data());

    if (msg->Record.GetStatus() == NKikimrProto::EReplyStatus::OK) {
        Y_ABORT_UNLESS(msg->Record.GetResponses().size() == 32);

        TVector<IDirectBlockGroupPtr> directBlockGroups;
        for (size_t i = 0; i < 32; i++) {
            const auto& response = msg->Record.GetResponses()[i];
            Y_ABORT_UNLESS(response.GetDirectBlockGroupId() == i);
            Y_ABORT_UNLESS(response.GetActualNumVChunks() == 1);

            TVector<NBsController::TDDiskId> ddiskIds;
            TVector<NBsController::TDDiskId> persistentBufferDDiskIds;
            for (const auto& node: response.GetNodes()) {
                ddiskIds.emplace_back(node.GetDDiskId());
                persistentBufferDDiskIds.emplace_back(
                    node.GetPersistentBufferDDiskId());
            }

            directBlockGroups.emplace_back(std::make_shared<TDirectBlockGroup>(
                TActivationContext::ActorSystem(),
                TabletID(),
                1,                 // generation
                i, // directBlockGroupIndex
                std::move(ddiskIds),
                std::move(persistentBufferDDiskIds),
                VolumeConfig.GetBlockSize(),
                VolumeConfig.GetPartitions(0).GetBlockCount(),
                StorageConfig.GetSyncRequestsBatchSize()
            ));
            directBlockGroups[i]->EstablishConnections({});
        }

        auto region = std::make_shared<TRegion>(std::move(directBlockGroups));
        auto fastPathService = std::make_shared<TFastPathService>(
            TActivationContext::ActorSystem(),
            TabletID(),
            1,                 // generation
            std::move(region),
            StorageConfig,
            AppData()->Counters);

        LoadActorAdapter = CreateLoadActorAdapter(ctx.SelfID, fastPathService);

        {
            auto service = GetNbsService();

            TString diskId = VolumeConfig.GetDiskId();
            ui32 blockSize = VolumeConfig.GetBlockSize();
            ui64 blockCount = 0;
            for (const auto& p: VolumeConfig.GetPartitions()) {
                blockCount += p.GetBlockCount();
            }

            TString socketPath = "/tmp/" + diskId + ".sock";
            NVhost::TStorageOptions options{
                .DiskId = diskId,
                .ClientId = "client-1",
                .BlockSize = blockSize,
                .BlocksCount = blockCount,
                .VhostQueuesCount = 1};
            service->VhostServer->StartEndpoint(
                std::move(socketPath),
                fastPathService,
                options);
        }

        LOG_INFO(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "Started NBS partition LoadActorAdapter: actor id %s",
            LoadActorAdapter.ToString().data());
    } else {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_PARTITION,
            "HandleControllerAllocateDDiskBlockGroupResult finished with "
            "error: %d, reason: %s",
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
    response->ActorId = LoadActorAdapter.ToString();
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

///////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Handle UpdateVolumeConfig request"
                << ", tabletId: " << TabletID()
                << ", txId: " << msg->Record.GetTxId()
                << ", sender: " << ev->Sender
                << ", version: " << msg->Record.GetVolumeConfig().GetVersion());

    // Store DDiskPoolName from StoragePoolName
    if (msg->Record.GetVolumeConfig().HasStoragePoolName()) {
        const TString& storagePoolName = msg->Record.GetVolumeConfig().GetStoragePoolName();
        StorageConfig.SetDDiskPoolName(storagePoolName);
        StorageConfig.SetPersistentBufferDDiskPoolName(storagePoolName);

        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                    "Updated DDiskPoolName from StoragePoolName"
                    << ", tabletId: " << TabletID()
                    << ", storagePoolName: " << storagePoolName);
    }

    // Store volume config
    VolumeConfig.CopyFrom(msg->Record.GetVolumeConfig());

    // Send response back to volume
    auto response = std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(msg->Record.GetTxId());
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(NKikimrBlockStore::OK);

    LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                "Sending UpdateVolumeConfig response"
                << ", tabletId: " << TabletID()
                << ", txId: " << response->Record.GetTxId()
                << ", status: OK");

    ctx.Send(ev->Sender, response.release());

    // TODO: make separate state
    AllocateDDiskBlockGroup(ctx);
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
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
        HFunc(NKikimr::TEvBlockStore::TEvUpdateVolumeConfig, HandleUpdateVolumeConfig);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::NBS_PARTITION,
                    "Unhandled event type: " << ev->GetTypeRewrite()
                        << " event: " << ev->ToString());
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
