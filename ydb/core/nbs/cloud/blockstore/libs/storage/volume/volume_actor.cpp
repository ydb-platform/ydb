#include "volume_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_VOLUME

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

TVolumeActor::TVolumeActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfo* info)
    : TTabletExecutedFlat(info, tablet, new NKikimr::NMiniKQL::TMiniKQLFactory)
{}

void TVolumeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    YDB_LOG_INFO_CTX(ctx, "Started NBS volume: tablet id",
        {"#_SelfId", SelfId()});
}

void TVolumeActor::OnDetach(const TActorContext& ctx)
{
    YDB_LOG_DEBUG_CTX(ctx, "OnDetach");
    Die(ctx);
}

void TVolumeActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    YDB_LOG_DEBUG_CTX(ctx, "OnTabletDead");
    Die(ctx);
}

void TVolumeActor::OnActivateExecutor(const TActorContext& ctx)
{
    // RunTxInitSchema(ctx);
    YDB_LOG_INFO_CTX(ctx, "OnActivateExecutor: tablet id",
        {"tabletID", TabletID()});

    // allow pipes to connect
    SignalTabletActive(ctx);

    ReportTabletState(ctx);
}

void TVolumeActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    YDB_LOG_DEBUG_CTX(ctx, "DefaultSignalTabletActive");
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
    YDB_LOG_DEBUG_CTX(ctx, "Processing",
        {"event", ev->GetTypeName().data()},
        {"sender", ev->Sender.LocalId()});

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvTabletPipe::TEvServerDestroyed, HandleServerDestroyed);

        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse,
            HandleUpdateVolumeConfigResponse);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                YDB_LOG_DEBUG_CTX(ctx, "Unhandled event",
                    {"type", ev->GetTypeRewrite()},
                    {"event", ev->ToString()});
            }
            break;
    }
}

void TVolumeActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_DEBUG_CTX(ctx, "Pipe client server connected to volume",
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

void TVolumeActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_DEBUG_CTX(ctx, "Pipe client server disconnected from volume",
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

void TVolumeActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_INFO_CTX(ctx, "Pipe client server got destroyed for volume",
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

void TVolumeActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const ui64 txId = msg->Record.GetTxId();

    YDB_LOG_INFO_CTX(ctx, "Handle UpdateVolumeConfig request",
        {"tabletId", TabletID()},
        {"txId", txId},
        {"sender", ev->Sender},
        {"partitions", msg->Record.PartitionsSize()},
        {"version", msg->Record.GetVolumeConfig().GetVersion()});

    // Store request info
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<NBlockStore::TCallContext>());

    TUpdateVolumeConfigRequest& request = UpdateVolumeConfigRequests[txId];
    request.RequestInfo = std::move(requestInfo);
    request.TxId = txId;

    Y_ABORT_UNLESS(msg->Record.GetPartitions().size() == 1);

    // Forward the event to all partitions
    for (const auto& partition: msg->Record.GetPartitions()) {
        ui64 partitionTabletId = partition.GetTabletId();

        YDB_LOG_INFO_CTX(ctx, "Forwarding UpdateVolumeConfig to partition",
            {"partitionId", partition.GetPartitionId()},
            {"tabletId", partitionTabletId});

        auto forwardEvent =
            std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig>();
        forwardEvent->Record.CopyFrom(msg->Record);

        // Create pipe client and send the event
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 3};
        auto pipeClient = ctx.Register(NTabletPipe::CreateClient(
            ctx.SelfID,
            partitionTabletId,
            clientConfig));
        NTabletPipe::SendData(ctx, pipeClient, forwardEvent.release());

        // Store pipe client for later cleanup
        request.PartitionPipes[partitionTabletId] = pipeClient;
        request.PendingPartitions.insert(partitionTabletId);
    }
}

void TVolumeActor::HandleUpdateVolumeConfigResponse(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 txId = msg->Record.GetTxId();
    const ui64 partitionTabletId = msg->Record.GetOrigin();

    YDB_LOG_INFO_CTX(ctx, "Handle UpdateVolumeConfigResponse",
        {"tabletId", TabletID()},
        {"txId", txId},
        {"partitionTabletId", partitionTabletId},
        {"status", static_cast<int>(msg->Record.GetStatus())});

    auto it = UpdateVolumeConfigRequests.find(txId);
    if (it == UpdateVolumeConfigRequests.end()) {
        YDB_LOG_WARN_CTX(ctx, "Received UpdateVolumeConfigResponse for unknown txId",
            {"txId", txId});
        return;
    }

    TUpdateVolumeConfigRequest& request = it->second;

    // Remove partition from pending set
    request.PendingPartitions.erase(partitionTabletId);

    // Close pipe to this partition
    auto pipeIt = request.PartitionPipes.find(partitionTabletId);
    if (pipeIt != request.PartitionPipes.end()) {
        NTabletPipe::CloseClient(ctx, pipeIt->second);
        request.PartitionPipes.erase(pipeIt);
    }

    // Send response to original sender
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(txId);
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(msg->Record.GetStatus());

    YDB_LOG_INFO_CTX(ctx, "Sending UpdateVolumeConfig response",
        {"tabletId", TabletID()},
        {"txId", txId},
        {"status", static_cast<int>(msg->Record.GetStatus())});

    NYdb::NBS::Reply(ctx, *request.RequestInfo, std::move(response));

    // Cleanup request
    UpdateVolumeConfigRequests.erase(it);
}

}   // namespace NYdb::NBS::NStorage
