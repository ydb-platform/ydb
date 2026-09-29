#include "volume_actor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

// The user state reported to the node whiteboard; the same value as the
// partition tablet's STATE_WORK.
constexpr ui32 WhiteboardUserStateWork = 2;

// A pipe to the partition gives up after a few retries so that the requests
// it carries fail fast and their senders retry.
NTabletPipe::TClientConfig MakePartitionPipeConfig()
{
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {.RetryLimitCount = 3};
    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeActor::TVolumeActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , NBlockStore::NStorage::TTabletBase<TVolumeActor>(
          tablet,
          NKikimr::TTabletStorageInfoPtr(info),
          nullptr)
{}

void TVolumeActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev)
{
    StateInitImpl(ev, SelfId());
}

STFUNC(TVolumeActor::StateBoot)
{
    switch (ev->GetTypeRewrite()) {
        case TNbs1Service::TEvStatVolumeRequest::EventType:
        case TNbs1Volume::TEvWaitReadyRequest::EventType:
        case NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::EventType:
            PostponedEvents.emplace_back(ev.Release());
            break;

        default:
            HandleCommonEvents(ev);
            break;
    }
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
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse,
            HandleUpdateVolumeConfigResponse);

        HFunc(TNbs1Service::TEvStatVolumeRequest, HandleStatVolume);
        HFunc(TNbs1Service::TEvStatVolumeResponse, HandleStatVolumeResponse);
        HFunc(TNbs1Volume::TEvWaitReadyRequest, HandleWaitReady);
        HFunc(TNbs1Volume::TEvWaitReadyResponse, HandleWaitReadyResponse);

        default:
            HandleCommonEvents(ev);
            break;
    }
}

void TVolumeActor::OnDetach(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "OnDetach");
    CleanupResources(ctx);
    Die(ctx);
}

void TVolumeActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "OnTabletDead");
    CleanupResources(ctx);
    Die(ctx);
}

void TVolumeActor::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateBoot);

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "OnActivateExecutor: tablet id %lu",
        TabletID());

    if (!Executor()->GetStats().IsFollower()) {
        ExecuteTx(ctx, CreateTx<TInitSchema>());
    }

    // allow pipes to connect
    SignalTabletActive(ctx);

    ReportTabletState(ctx);
}

void TVolumeActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NBS_VOLUME, "DefaultSignalTabletActive");
}

void TVolumeActor::CleanupResources(const TActorContext& ctx)
{
    for (const auto& item: UpdateVolumeConfigRequests) {
        NTabletPipe::CloseClient(ctx, item.second.PartitionPipe);
    }
    UpdateVolumeConfigRequests.clear();

    DropPartitionPipe(ctx, "volume tablet is shutting down");
}

void TVolumeActor::ReportTabletState(const TActorContext& ctx)
{
    auto service =
        NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request =
        std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
            TabletID(),
            WhiteboardUserStateWork);

    NYdb::NBS::Send(ctx, service, std::move(request));
}

void TVolumeActor::HandleCommonEvents(TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvTabletPipe::TEvServerDestroyed, HandleServerDestroyed);
        HFunc(TEvTabletPipe::TEvClientConnected, HandleClientConnected);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleClientDestroyed);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG_S(
                    NActors::TActivationContext::AsActorContext(),
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
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Status == NKikimrProto::OK) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "Pipe client %s connected to tablet %lu",
            ToString(msg->ClientId).c_str(),
            msg->TabletId);
        return;
    }

    LOG_WARN(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Pipe client %s failed to connect to tablet %lu: %s",
        ToString(msg->ClientId).c_str(),
        msg->TabletId,
        NKikimrProto::EReplyStatus_Name(msg->Status).c_str());

    if (msg->ClientId == PartitionPipe) {
        DropPartitionPipe(ctx, "pipe to the partition failed");
    }
}

void TVolumeActor::HandleClientDestroyed(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Pipe client %s to tablet %lu destroyed",
        ToString(msg->ClientId).c_str(),
        msg->TabletId);

    if (msg->ClientId == PartitionPipe) {
        DropPartitionPipe(ctx, "pipe to the partition failed");
    }
}

void TVolumeActor::DropPartitionPipe(
    const TActorContext& ctx,
    const TString& reason)
{
    if (PartitionPipe) {
        NTabletPipe::CloseClient(ctx, PartitionPipe);
        PartitionPipe = {};
    }

    THashMap<ui64, TForwardedRequest> requests;
    requests.swap(ForwardedRequests);

    const auto error = MakeError(E_REJECTED, reason);
    for (const auto& [cookie, request]: requests) {
        RejectForwardedRequest(ctx, request, error);
    }
}

void TVolumeActor::SendPostponedEvents(const TActorContext& ctx)
{
    TVector<std::unique_ptr<NActors::IEventHandle>> events;
    events.swap(PostponedEvents);

    for (auto& ev: events) {
        ctx.Send(std::move(ev));
    }
}

void TVolumeActor::HandleUpdateVolumeConfig(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 txId = msg->Record.GetTxId();

    Y_ABORT_UNLESS(msg->Record.PartitionsSize() == 1);
    const auto& partition = msg->Record.GetPartitions(0);
    const ui64 partitionTabletId = partition.GetTabletId();

    LOG_INFO_S(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Handle UpdateVolumeConfig request"
            << ", tabletId: " << TabletID() << ", txId: " << txId
            << ", sender: " << ev->Sender
            << ", partitionId: " << partition.GetPartitionId()
            << ", partitionTabletId: " << partitionTabletId
            << ", version: " << msg->Record.GetVolumeConfig().GetVersion());

    // A redelivered transaction starts over: the earlier forward may have been
    // lost together with its pipe.
    if (auto it = UpdateVolumeConfigRequests.find(txId);
        it != UpdateVolumeConfigRequests.end())
    {
        NTabletPipe::CloseClient(ctx, it->second.PartitionPipe);
        UpdateVolumeConfigRequests.erase(it);
    }

    auto forwardEvent =
        std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig>();
    forwardEvent->Record.CopyFrom(msg->Record);

    const TActorId partitionPipe = ctx.Register(NTabletPipe::CreateClient(
        ctx.SelfID,
        partitionTabletId,
        MakePartitionPipeConfig()));
    NTabletPipe::SendData(ctx, partitionPipe, forwardEvent.release());

    UpdateVolumeConfigRequests[txId] = TUpdateVolumeConfigRequest{
        .RequestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            MakeIntrusive<NBlockStore::TCallContext>()),
        .PartitionTabletId = partitionTabletId,
        .PartitionPipe = partitionPipe,
    };
}

void TVolumeActor::HandleUpdateVolumeConfigResponse(
    const NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 txId = msg->Record.GetTxId();
    const NKikimrBlockStore::EStatus status = msg->Record.GetStatus();

    LOG_INFO_S(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Handle UpdateVolumeConfigResponse"
            << ", tabletId: " << TabletID() << ", txId: " << txId
            << ", partitionTabletId: " << msg->Record.GetOrigin()
            << ", status: " << NKikimrBlockStore::EStatus_Name(status));

    auto it = UpdateVolumeConfigRequests.find(txId);
    if (it == UpdateVolumeConfigRequests.end()) {
        LOG_WARN_S(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "Received UpdateVolumeConfigResponse for unknown txId" << ", txId: "
                                                                   << txId);
        return;
    }

    TUpdateVolumeConfigRequest request = std::move(it->second);
    UpdateVolumeConfigRequests.erase(it);
    NTabletPipe::CloseClient(ctx, request.PartitionPipe);

    if (status != NKikimrBlockStore::OK) {
        ReplyUpdateVolumeConfig(ctx, *request.RequestInfo, txId, status);
        return;
    }

    if (request.PartitionTabletId == PartitionTabletId) {
        ReplyUpdateVolumeConfig(ctx, *request.RequestInfo, txId, status);
        return;
    }

    // The reply follows the commit so that the id survives a restart.
    ExecuteTx(
        ctx,
        CreateTx<TStorePartitionTabletId>(
            request.PartitionTabletId,
            txId,
            std::move(request.RequestInfo)));
}

void TVolumeActor::ReplyUpdateVolumeConfig(
    const NActors::TActorContext& ctx,
    const TRequestInfo& requestInfo,
    ui64 txId,
    NKikimrBlockStore::EStatus status)
{
    auto response = std::make_unique<
        NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    response->Record.SetTxId(txId);
    response->Record.SetOrigin(TabletID());
    response->Record.SetStatus(status);

    LOG_INFO_S(
        ctx,
        NKikimrServices::NBS_VOLUME,
        "Sending UpdateVolumeConfig response"
            << ", tabletId: " << TabletID() << ", txId: " << txId
            << ", status: " << NKikimrBlockStore::EStatus_Name(status));

    NYdb::NBS::Reply(ctx, requestInfo, std::move(response));
}

void TVolumeActor::HandleStatVolume(
    const TNbs1Service::TEvStatVolumeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardToPartition<TNbs1Service::TEvStatVolumeRequest>(
        ctx,
        ev,
        EForwardedRequestKind::StatVolume);
}

void TVolumeActor::HandleStatVolumeResponse(
    const TNbs1Service::TEvStatVolumeResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    RelayPartitionResponse<TNbs1Service::TEvStatVolumeResponse>(ctx, ev);
}

void TVolumeActor::HandleWaitReady(
    const TNbs1Volume::TEvWaitReadyRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    ForwardToPartition<TNbs1Volume::TEvWaitReadyRequest>(
        ctx,
        ev,
        EForwardedRequestKind::WaitReady);
}

void TVolumeActor::HandleWaitReadyResponse(
    const TNbs1Volume::TEvWaitReadyResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    RelayPartitionResponse<TNbs1Volume::TEvWaitReadyResponse>(ctx, ev);
}

template <typename TRequest>
void TVolumeActor::ForwardToPartition(
    const NActors::TActorContext& ctx,
    const typename TRequest::TPtr& ev,
    EForwardedRequestKind kind)
{
    const TForwardedRequest request{
        .Sender = ev->Sender,
        .Cookie = ev->Cookie,
        .Kind = kind,
    };

    if (!PartitionTabletId) {
        LOG_DEBUG(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "[%lu] Request rejected: partition tablet is not known",
            TabletID());

        RejectForwardedRequest(
            ctx,
            request,
            MakeError(E_REJECTED, "partition tablet is not known"));
        return;
    }

    if (!PartitionPipe) {
        PartitionPipe = ctx.Register(NTabletPipe::CreateClient(
            ctx.SelfID,
            PartitionTabletId,
            MakePartitionPipeConfig()));
    }

    auto forwardEvent = std::make_unique<TRequest>();
    forwardEvent->Record.CopyFrom(ev->Get()->Record);

    const ui64 forwardCookie = ++LastForwardCookie;
    ForwardedRequests[forwardCookie] = request;
    NTabletPipe::SendData(
        ctx,
        PartitionPipe,
        forwardEvent.release(),
        forwardCookie);
}

template <typename TResponse>
void TVolumeActor::RelayPartitionResponse(
    const NActors::TActorContext& ctx,
    const typename TResponse::TPtr& ev)
{
    const auto it = ForwardedRequests.find(ev->Cookie);
    if (it == ForwardedRequests.end()) {
        LOG_WARN(
            ctx,
            NKikimrServices::NBS_VOLUME,
            "[%lu] Partition response for unknown cookie %lu",
            TabletID(),
            ev->Cookie);
        return;
    }

    const TForwardedRequest request = it->second;
    ForwardedRequests.erase(it);

    auto response = std::make_unique<TResponse>();
    response->Record.CopyFrom(ev->Get()->Record);
    ctx.Send(request.Sender, response.release(), 0, request.Cookie);
}

void TVolumeActor::RejectForwardedRequest(
    const NActors::TActorContext& ctx,
    const TForwardedRequest& request,
    const NProto::TError& error)
{
    std::unique_ptr<IEventBase> response;
    switch (request.Kind) {
        case EForwardedRequestKind::StatVolume:
            response =
                std::make_unique<TNbs1Service::TEvStatVolumeResponse>(error);
            break;
        case EForwardedRequestKind::WaitReady:
            response =
                std::make_unique<TNbs1Volume::TEvWaitReadyResponse>(error);
            break;
    }

    ctx.Send(request.Sender, response.release(), 0, request.Cookie);
}

}   // namespace NYdb::NBS::NStorage
