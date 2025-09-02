#include "msgbus_server_request.h"
#include "msgbus_securereq.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/base/ticket_parser.h>

namespace NKikimr {
namespace NMsgBusProxy {

using namespace NKikimrCms;
using namespace NCms;

namespace {

class TCmsRequestActor : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TCmsRequestActor>>
{
    using TActorBase = TActorBootstrapped<TCmsRequestActor>;
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TCmsRequestActor>>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TCmsRequestActor(NKikimrClient::TCmsRequest &request, NMsgBusProxy::TBusMessageContext &msg)
        : TBase(msg)
        , Request(request)
    {
        TBase::SetSecurityToken(request.GetSecurityToken());
        TBase::SetPeerName(msg.GetPeerName());
        TBase::SetRequireAdminAccess(true);
    }

    void Bootstrap(const TActorContext &ctx)
    {
        auto dinfo = AppData(ctx)->DomainsInfo;

        if (Request.HasDomainName() && (!dinfo->Domain || dinfo->GetDomain()->Name != Request.GetDomainName())) {
            auto error = Sprintf("Unknown domain %s", Request.GetDomainName().data());
            ReplyWithErrorAndDie(error, ctx);
            return;
        }

        SendRequest(ctx);
        Become(&TCmsRequestActor::MainState);
    }

    void SendRequest(const TActorContext &ctx)
    {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 10};
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, MakeCmsID(), pipeConfig);
        CmsPipe = ctx.RegisterWithSameMailbox(pipe);

        LOG_DEBUG(ctx, NKikimrServices::CMS, "Forwarding CMS request: %s",
                  Request.ShortDebugString().data());

        if (Request.HasClusterStateRequest()) {
            TAutoPtr<TEvCms::TEvClusterStateRequest> request
                = new TEvCms::TEvClusterStateRequest;
            request->Record.CopyFrom(Request.GetClusterStateRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasPermissionRequest()) {
            TAutoPtr<TEvCms::TEvPermissionRequest> request
                = new TEvCms::TEvPermissionRequest;
            request->Record.CopyFrom(Request.GetPermissionRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasManageRequestRequest()) {
            TAutoPtr<TEvCms::TEvManageRequestRequest> request
                = new TEvCms::TEvManageRequestRequest;
            request->Record.CopyFrom(Request.GetManageRequestRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasCheckRequest()) {
            TAutoPtr<TEvCms::TEvCheckRequest> request
                = new TEvCms::TEvCheckRequest;
            request->Record.CopyFrom(Request.GetCheckRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasManagePermissionRequest()) {
            TAutoPtr<TEvCms::TEvManagePermissionRequest> request
                = new TEvCms::TEvManagePermissionRequest;
            request->Record.CopyFrom(Request.GetManagePermissionRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasNotification()) {
            TAutoPtr<TEvCms::TEvNotification> request
                = new TEvCms::TEvNotification;
            request->Record.CopyFrom(Request.GetNotification());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasManageNotificationRequest()) {
            TAutoPtr<TEvCms::TEvManageNotificationRequest> request
                = new TEvCms::TEvManageNotificationRequest;
            request->Record.CopyFrom(Request.GetManageNotificationRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasGetConfigRequest()) {
            TAutoPtr<TEvCms::TEvGetConfigRequest> request
                = new TEvCms::TEvGetConfigRequest;
            request->Record.CopyFrom(Request.GetGetConfigRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasGetLogTailRequest()) {
            TAutoPtr<TEvCms::TEvGetLogTailRequest> request
                = new TEvCms::TEvGetLogTailRequest;
            request->Record.CopyFrom(Request.GetGetLogTailRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasResetMarkerRequest()) {
            TAutoPtr<TEvCms::TEvResetMarkerRequest> request
                = new TEvCms::TEvResetMarkerRequest;
            request->Record.CopyFrom(Request.GetResetMarkerRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasSetConfigRequest()) {
            TAutoPtr<TEvCms::TEvSetConfigRequest> request
                = new TEvCms::TEvSetConfigRequest;
            request->Record.CopyFrom(Request.GetSetConfigRequest());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else if (Request.HasSetMarkerRequest()) {
            TAutoPtr<TEvCms::TEvSetMarkerRequest> request
                = new TEvCms::TEvSetMarkerRequest;
            request->Record.CopyFrom(Request.GetSetMarkerRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else {
            ReplyWithErrorAndDie("Unknown CMS request", ctx);
        }
    }

    void Handle(TEvCms::TEvClusterStateResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableClusterStateResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvPermissionResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutablePermissionResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvManageRequestResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableManageRequestResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvManagePermissionResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableManagePermissionResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvNotificationResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableNotificationResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvManageNotificationResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableManageNotificationResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvGetConfigResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableGetConfigResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvGetLogTailResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableGetLogTailResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvResetMarkerResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableResetMarkerResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvSetConfigResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableSetConfigResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvCms::TEvSetMarkerResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableSetMarkerResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Undelivered(const TActorContext &ctx) {
        ReplyWithErrorAndDie("CMS is unavailable", ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) noexcept
    {
        if (ev->Get()->Status != NKikimrProto::OK)
            Undelivered(ctx);
    }

    void Die(const TActorContext &ctx)
    {
        if (CmsPipe)
            NTabletPipe::CloseClient(ctx, CmsPipe);
        TBase::Die(ctx);
    }

    void SendReplyAndDie(const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(Response.HasStatus());

        auto response = MakeHolder<TBusCmsResponse>();
        response->Record = std::move(Response);
        SendReplyMove(response.Release());
        Die(ctx);
    }

    void ReplyWithErrorAndDie(const TString &error, const TActorContext &ctx)
    {
        Response.MutableStatus()->SetCode(TStatus::ERROR);
        Response.MutableStatus()->SetReason(error);
        SendReplyAndDie(ctx);
    }

    void ReplyWithErrorAndDie(const TStatus &status, const TActorContext &ctx)
    {
        Response.MutableStatus()->CopyFrom(status);
        SendReplyAndDie(ctx);
    }

    STFUNC(MainState) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            HFunc(TEvCms::TEvClusterStateResponse, Handle);
            HFunc(TEvCms::TEvPermissionResponse, Handle);
            HFunc(TEvCms::TEvManageRequestResponse, Handle);
            HFunc(TEvCms::TEvManagePermissionResponse, Handle);
            HFunc(TEvCms::TEvNotificationResponse, Handle);
            HFunc(TEvCms::TEvManageNotificationResponse, Handle);
            HFunc(TEvCms::TEvGetConfigResponse, Handle);
            HFunc(TEvCms::TEvGetLogTailResponse, Handle);
            HFunc(TEvCms::TEvResetMarkerResponse, Handle);
            HFunc(TEvCms::TEvSetConfigResponse, Handle);
            HFunc(TEvCms::TEvSetMarkerResponse, Handle);
            CFunc(TEvTabletPipe::EvClientDestroyed, Undelivered);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        default:
            Y_ABORT("TCmsRequestActor::MainState unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(),
                   ev->ToString().data());
        }
    }

private:
    NKikimrClient::TCmsRequest Request;
    NKikimrClient::TCmsResponse Response;
    TActorId CmsPipe;
};

} // namespace

IActor* CreateMessageBusCmsRequest(TBusMessageContext &msg)
{
    NKikimrClient::TCmsRequest &record
        = static_cast<TBusCmsRequest*>(msg.GetMessage())->Record;
    return new TCmsRequestActor(record, msg);
}

} // namespace NMsgBusProxy
} // namespace NKikimr
