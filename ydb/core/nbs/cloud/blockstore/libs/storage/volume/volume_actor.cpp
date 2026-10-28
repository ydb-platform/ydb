#include "volume_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/core/event_pb.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

TVolumeActor::TVolumeActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfo* info)
    : NBlockStore::NStorage::TTabletBase<TVolumeActor>(
          tablet,
          NKikimr::TTabletStorageInfoPtr(info),
          nullptr)
{}

void TVolumeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Started NBS volume: tablet id %s",
        SelfId().ToString().data());
}

void TVolumeActor::OnDetach(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "OnDetach");
    Die(ctx);
}

void TVolumeActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "OnTabletDead");
    Die(ctx);
}

void TVolumeActor::OnActivateExecutor(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "OnActivateExecutor: tablet id %lu",
        TabletID());

    ExecuteTx(ctx, CreateTx<TInitSchema>());

    ReportTabletState(ctx);
}

void TVolumeActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "DefaultSignalTabletActive");
}

void TVolumeActor::ReportTabletState(const TActorContext& ctx)
{
    auto service =
        NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request =
        std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
            TabletID(),
            STATE_WORK);

    NYdb::NBS::Send(ctx, service, std::move(request));
}

STFUNC(TVolumeActor::StateWork)
{
    auto ctx = NActors::TActivationContext::AsActorContext();
    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvTabletPipe::TEvServerDestroyed, HandleServerDestroyed);

        HFunc(TEvTabletPipe::TEvClientConnected, HandleClientConnected);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleClientDestroyed);

        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse,
            HandleUpdateVolumeConfigResponse);

        HFunc(
            NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeRequest,
            HandleStatVolume);
        HFunc(
            NNbs1CompatApi::NBlockStore::TEvVolume::TEvWaitReadyRequest,
            HandleWaitReady);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG_S(
                    ctx,
                    NKikimrServices::NBS_VOLUME,
                    "Unhandled event type: " << ev->GetTypeRewrite()
                                             << " event: " << ev->ToString());
            }
            break;
    }
}

void TVolumeActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Pipe client %s server %s connected to volume",
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TVolumeActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Pipe client %s server %s disconnected from volume",
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TVolumeActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Pipe client %s server %s got destroyed for volume",
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TVolumeActor::HandleClientConnected(
    const TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (msg->Status == NKikimrProto::OK) {
        return;
    }

    ResendPendingEventsToPartition(ctx, msg->ClientId);
}

void TVolumeActor::HandleClientDestroyed(
    const TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    ResendPendingEventsToPartition(ctx, ev->Get()->ClientId);
}

ui64 TVolumeActor::SendPendingEventToPartition(
    const TActorContext& ctx,
    ui64 partitionTabletId,
    std::unique_ptr<IEventBase> event)
{
    TAllocChunkSerializer serializer;
    Y_ABORT_UNLESS(event->SerializeToArcadiaStream(&serializer));

    TPendingEvent pendingEvent{
        .EventType = event->Type(),
        .Data = serializer.Release(event->CreateSerializationInfo(false)),
    };

    if (PartitionTabletId == 0) {
        PartitionTabletId = partitionTabletId;
    } else {
        Y_ABORT_UNLESS(PartitionTabletId == partitionTabletId);
    }

    const ui64 pendingEventId = NextPendingEventId++;
    const auto& stored =
        PendingEvents.emplace(pendingEventId, std::move(pendingEvent))
            .first->second;

    if (!PartitionPipeClient) {
        OpenPartitionPipe(ctx);
    }

    NTabletPipe::SendData(
        ctx,
        PartitionPipeClient,
        stored.EventType,
        stored.Data);
    return pendingEventId;
}

void TVolumeActor::OpenPartitionPipe(const TActorContext& ctx)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    PartitionPipeClient = ctx.Register(
        NTabletPipe::CreateClient(ctx.SelfID, PartitionTabletId, clientConfig));
}

void TVolumeActor::ResendPendingEventsToPartition(
    const TActorContext& ctx,
    const TActorId& pipeClient)
{
    if (pipeClient != PartitionPipeClient) {
        LOG_DEBUG_S(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "Ignoring stale partition pipe event"
                << ", tabletId: " << TabletID()
                << ", pipeClient: " << pipeClient);
        return;
    }

    LOG_WARN_S(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Resending pending events after pipe failure"
            << ", tabletId: " << TabletID() << ", partitionTabletId: "
            << PartitionTabletId << ", pendingEvents: " << PendingEvents.size()
            << ", pipeClient: " << pipeClient);

    NTabletPipe::CloseClient(ctx, PartitionPipeClient);
    PartitionPipeClient = {};

    if (PendingEvents.empty()) {
        return;
    }

    OpenPartitionPipe(ctx);
    for (const auto& [pendingEventId, pendingEvent]: PendingEvents) {
        Y_UNUSED(pendingEventId);
        NTabletPipe::SendData(
            ctx,
            PartitionPipeClient,
            pendingEvent.EventType,
            pendingEvent.Data);
    }
}

void TVolumeActor::ReleasePendingEvent(
    const TActorContext& ctx,
    ui64 pendingEventId)
{
    PendingEvents.erase(pendingEventId);
    if (!PendingEvents.empty() || !PartitionPipeClient) {
        return;
    }

    NTabletPipe::CloseClient(ctx, PartitionPipeClient);
    PartitionPipeClient = {};
}

void TVolumeActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const ui64 txId = msg->Record.GetTxId();

    LOG_INFO_S(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Handle UpdateVolumeConfig request"
            << ", tabletId: " << TabletID() << ", txId: " << txId
            << ", sender: " << ev->Sender
            << ", partitions: " << msg->Record.PartitionsSize()
            << ", version: " << msg->Record.GetVolumeConfig().GetVersion());

    // Store request info
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<NBlockStore::TCallContext>());

    auto [it, inserted] = UpdateVolumeConfigRequests.try_emplace(txId);
    TUpdateVolumeConfigRequest& request = it->second;
    // A repeated TxId is SchemeShard resending after its pipe broke. Reply
    // later to this latest sender and keep the pipe that is already open.
    request.RequestInfo = std::move(requestInfo);
    if (!inserted) {
        LOG_INFO_S(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "Repeated UpdateVolumeConfig for the pending request"
                << ", tabletId: " << TabletID() << ", txId: " << txId
                << ", sender: " << ev->Sender);
        return;
    }

    request.TxId = txId;

    Y_ABORT_UNLESS(msg->Record.GetPartitions().size() == 1);

    const ui64 partitionTabletId = msg->Record.GetPartitions(0).GetTabletId();
    ExecuteTx(
        ctx,
        CreateTx<TStorePartitionTabletId>(
            partitionTabletId,
            std::move(msg->Record)));
}

void TVolumeActor::ForwardUpdateVolumeConfig(
    const NActors::TActorContext& ctx,
    const NKikimrBlockStore::TUpdateVolumeConfig& record)
{
    const ui64 txId = record.GetTxId();
    auto it = UpdateVolumeConfigRequests.find(txId);
    if (it == UpdateVolumeConfigRequests.end()) {
        LOG_WARN_S(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "UpdateVolumeConfig already answered, txId: " << txId);
        return;
    }
    TUpdateVolumeConfigRequest& request = it->second;

    // Forward the event to all partitions
    for (const auto& partition: record.GetPartitions()) {
        ui64 partitionTabletId = partition.GetTabletId();

        LOG_INFO_S(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "Forwarding UpdateVolumeConfig to partition"
                << ", partitionId: " << partition.GetPartitionId()
                << ", tabletId: " << partitionTabletId);

        auto event =
            std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig>();
        event->Record.CopyFrom(record);
        request.PendingEventId = SendPendingEventToPartition(
            ctx,
            partitionTabletId,
            std::move(event));
    }
}

void TVolumeActor::HandleUpdateVolumeConfigResponse(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 txId = msg->Record.GetTxId();
    const ui64 partitionTabletId = msg->Record.GetOrigin();

    LOG_INFO_S(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Handle UpdateVolumeConfigResponse"
            << ", tabletId: " << TabletID() << ", txId: " << txId
            << ", partitionTabletId: " << partitionTabletId
            << ", status: " << static_cast<int>(msg->Record.GetStatus()));

    auto it = UpdateVolumeConfigRequests.find(txId);
    if (it == UpdateVolumeConfigRequests.end()) {
        LOG_WARN_S(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "Received UpdateVolumeConfigResponse for unknown txId" << ", txId: "
                                                                   << txId);
        return;
    }

    TUpdateVolumeConfigRequest& request = it->second;

    ReleasePendingEvent(ctx, request.PendingEventId);

    // Send response to original sender
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(txId);
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(msg->Record.GetStatus());

    LOG_INFO_S(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Sending UpdateVolumeConfig response"
            << ", tabletId: " << TabletID() << ", txId: " << txId
            << ", status: " << static_cast<int>(msg->Record.GetStatus()));

    NYdb::NBS::Reply(ctx, *request.RequestInfo, std::move(response));

    // Cleanup request
    UpdateVolumeConfigRequests.erase(it);
}

void TVolumeActor::HandleStatVolume(
    const NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeRequest::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "Handle StatVolume request");

    auto response = std::make_unique<
        NNbs1CompatApi::NBlockStore::TEvService::TEvStatVolumeResponse>();
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TVolumeActor::HandleWaitReady(
    const NNbs1CompatApi::NBlockStore::TEvVolume::TEvWaitReadyRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "Handle WaitReady request");

    auto response = std::make_unique<
        NNbs1CompatApi::NBlockStore::TEvVolume::TEvWaitReadyResponse>();
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

}   // namespace NYdb::NBS::NStorage
