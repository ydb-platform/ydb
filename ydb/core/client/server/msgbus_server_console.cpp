#include "msgbus_server_request.h"
#include "msgbus_securereq.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/ticket_parser.h>

namespace NKikimr {
namespace NMsgBusProxy {

using namespace NKikimrConsole;
using namespace NConsole;

namespace {

class TConsoleRequestActor : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TConsoleRequestActor>>
{
    using TActorBase = TActorBootstrapped<TConsoleRequestActor>;
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TConsoleRequestActor>>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TConsoleRequestActor(NKikimrClient::TConsoleRequest &request, NMsgBusProxy::TBusMessageContext &msg)
        : TBase(msg)
        , Request(request)
    {
        const auto& token = request.GetSecurityToken();
        if (!token.empty()) {
            TBase::SetSecurityToken(token);
        } else {
            const auto& clientCertificates = msg.FindClientCert();
            if (!clientCertificates.empty()) {
                TBase::SetSecurityToken(TString(clientCertificates.front()));
            }
        }
        // Don`t require admin access for GetNodeConfigRequest
        if (Request.GetRequestCase() != NKikimrClient::TConsoleRequest::kGetNodeConfigRequest) {
            TBase::SetRequireAdminAccess(true);
        }

    }

    void Bootstrap(const TActorContext &ctx)
    {
        auto dinfo = AppData(ctx)->DomainsInfo;

        if (Request.HasDomainName()) {
            auto *domain = dinfo->GetDomainByName(Request.GetDomainName());
            if (!domain) {
                auto error = Sprintf("Unknown domain %s", Request.GetDomainName().data());
                ReplyWithErrorAndDie(error, ctx);
                return;
            }
            StateStorageGroup = dinfo->GetDefaultStateStorageGroup(domain->DomainUid);
        } else {
            if (dinfo->Domains.size() > 1) {
                auto error = "Ambiguous domain (use --domain option)";
                ReplyWithErrorAndDie(error, ctx);
                return;
            }

            auto domain = dinfo->Domains.begin()->second;
            StateStorageGroup = dinfo->GetDefaultStateStorageGroup(domain->DomainUid);
        }

        SendRequest(ctx);
        TBase::Become(&TConsoleRequestActor::MainState);
    }

    void SendRequest(const TActorContext &ctx)
    {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 10};
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, MakeConsoleID(StateStorageGroup), pipeConfig);
        ConsolePipe = ctx.RegisterWithSameMailbox(pipe);

        // Don't print security token.
        Request.ClearSecurityToken();
        LOG_DEBUG(ctx, NKikimrServices::CMS, "Forwarding console request: %s",
                  Request.ShortDebugString().data());

        if (Request.HasCreateTenantRequest()) {
            auto request = MakeHolder<TEvConsole::TEvCreateTenantRequest>();
            request->Record.CopyFrom(Request.GetCreateTenantRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasGetConfigRequest()) {
            auto request = MakeHolder<TEvConsole::TEvGetConfigRequest>();
            request->Record.CopyFrom(Request.GetGetConfigRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasGetTenantStatusRequest()) {
            auto request = MakeHolder<TEvConsole::TEvGetTenantStatusRequest>();
            request->Record.CopyFrom(Request.GetGetTenantStatusRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasAlterTenantRequest()) {
            auto request = MakeHolder<TEvConsole::TEvAlterTenantRequest>();
            request->Record.CopyFrom(Request.GetAlterTenantRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasListTenantsRequest()) {
            auto request = MakeHolder<TEvConsole::TEvListTenantsRequest>();
            request->Record.CopyFrom(Request.GetListTenantsRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasRemoveTenantRequest()) {
            auto request = MakeHolder<TEvConsole::TEvRemoveTenantRequest>();
            request->Record.CopyFrom(Request.GetRemoveTenantRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasSetConfigRequest()) {
            auto request = MakeHolder<TEvConsole::TEvSetConfigRequest>();
            request->Record.CopyFrom(Request.GetSetConfigRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasConfigureRequest()) {
            auto request = MakeHolder<TEvConsole::TEvConfigureRequest>();
            request->Record.CopyFrom(Request.GetConfigureRequest());
            request->Record.SetUserToken(TBase::GetSerializedToken());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasGetConfigItemsRequest()) {
            auto request = MakeHolder<TEvConsole::TEvGetConfigItemsRequest>();
            request->Record.CopyFrom(Request.GetGetConfigItemsRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasGetNodeConfigItemsRequest()) {
            auto request = MakeHolder<TEvConsole::TEvGetNodeConfigItemsRequest>();
            request->Record.CopyFrom(Request.GetGetNodeConfigItemsRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasGetNodeConfigRequest()) {
            if (!CheckAccessGetNodeConfig()) {
                ReplyWithErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, "Cannot get node config. Access denied. Node is not authorized", ctx);
                return;
            }
            auto request = MakeHolder<TEvConsole::TEvGetNodeConfigRequest>();
            request->Record.CopyFrom(Request.GetGetNodeConfigRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasGetOperationRequest()) {
            auto request = MakeHolder<TEvConsole::TEvGetOperationRequest>();
            request->Record.MutableRequest()->CopyFrom(Request.GetGetOperationRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasCheckConfigUpdatesRequest()) {
            auto request = MakeHolder<TEvConsole::TEvCheckConfigUpdatesRequest>();
            request->Record.CopyFrom(Request.GetCheckConfigUpdatesRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasListConfigValidatorsRequest()) {
            auto request = MakeHolder<TEvConsole::TEvListConfigValidatorsRequest>();
            request->Record.CopyFrom(Request.GetListConfigValidatorsRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasToggleConfigValidatorRequest()) {
            auto request = MakeHolder<TEvConsole::TEvToggleConfigValidatorRequest>();
            request->Record.CopyFrom(Request.GetToggleConfigValidatorRequest());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else if (Request.HasUpdateTenantPoolConfig()) {
            auto request = MakeHolder<TEvConsole::TEvUpdateTenantPoolConfig>();
            request->Record.CopyFrom(Request.GetUpdateTenantPoolConfig());
            NTabletPipe::SendData(ctx, ConsolePipe, request.Release());
        } else {
            ReplyWithErrorAndDie("Unknown console request", ctx);
        }
    }

    void Handle(TEvConsole::TEvAlterTenantResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        auto &resp = rec.GetResponse();
        Response.MutableStatus()->SetCode(resp.operation().status());
        if (resp.operation().issues_size())
            Response.MutableStatus()->SetReason(resp.operation().issues(0).message());
        Response.MutableAlterTenantResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvCheckConfigUpdatesResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableCheckConfigUpdatesResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvConfigureResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableConfigureResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvCreateTenantResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        auto &resp = rec.GetResponse();
        Response.MutableStatus()->SetCode(resp.operation().status());
        if (resp.operation().issues_size())
            Response.MutableStatus()->SetReason(resp.operation().issues(0).message());
        Response.MutableCreateTenantResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvGetConfigItemsResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableGetConfigItemsResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvGetConfigResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        Response.MutableGetConfigResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvGetOperationResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        auto &resp = rec.GetResponse();
        Response.MutableStatus()->SetCode(resp.operation().status());
        if (resp.operation().issues_size())
            Response.MutableStatus()->SetReason(resp.operation().issues(0).message());
        Response.MutableGetOperationResponse()->CopyFrom(resp);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvGetTenantStatusResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        auto &resp = rec.GetResponse();
        Response.MutableStatus()->SetCode(resp.operation().status());
        if (resp.operation().issues_size())
            Response.MutableStatus()->SetReason(resp.operation().issues(0).message());
        Response.MutableGetTenantStatusResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvListConfigValidatorsResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        Response.MutableListConfigValidatorsResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvListTenantsResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);
        Response.MutableListTenantsResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvGetNodeConfigItemsResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableGetNodeConfigItemsResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvGetNodeConfigResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableGetNodeConfigResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvRemoveTenantResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        auto &resp = rec.GetResponse();
        Response.MutableStatus()->SetCode(resp.operation().status());
        if (resp.operation().issues_size())
            Response.MutableStatus()->SetReason(resp.operation().issues(0).message());
        Response.MutableRemoveTenantResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvSetConfigResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableSetConfigResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Handle(TEvConsole::TEvToggleConfigValidatorResponse::TPtr &ev, const TActorContext &ctx) noexcept
    {
        auto &rec = ev->Get()->Record;
        Response.MutableStatus()->CopyFrom(rec.GetStatus());
        Response.MutableToggleConfigValidatorResponse()->CopyFrom(rec);
        SendReplyAndDie(ctx);
    }

    void Undelivered(const TActorContext &ctx) {
        ReplyWithErrorAndDie(Ydb::StatusIds::UNAVAILABLE, "Console is unavailable", ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) noexcept
    {
        if (ev->Get()->Status != NKikimrProto::OK)
            Undelivered(ctx);
    }

    void Die(const TActorContext &ctx)
    {
        NTabletPipe::CloseClient(ctx, ConsolePipe);
        TBase::Die(ctx);
    }

    void SendReplyAndDie(const TActorContext &ctx)
    {
        Y_ABORT_UNLESS(Response.HasStatus());

        auto response = MakeHolder<TBusConsoleResponse>();
        response->Record = std::move(Response);
        SendReplyMove(response.Release());
        Die(ctx);
    }

    void ReplyWithErrorAndDie(const TString &error, const TActorContext &ctx)
    {
        ReplyWithErrorAndDie(Ydb::StatusIds::GENERIC_ERROR, error, ctx);
    }

    void ReplyWithErrorAndDie(Ydb::StatusIds::StatusCode code, const TString &error,
                              const TActorContext &ctx)
    {
        Response.MutableStatus()->SetCode(code);
        Response.MutableStatus()->SetReason(error);
        SendReplyAndDie(ctx);
    }

    STFUNC(MainState) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            HFunc(TEvConsole::TEvAlterTenantResponse, Handle);
            HFunc(TEvConsole::TEvConfigureResponse, Handle);
            HFunc(TEvConsole::TEvCreateTenantResponse, Handle);
            HFunc(TEvConsole::TEvGetConfigItemsResponse, Handle);
            HFunc(TEvConsole::TEvGetConfigResponse, Handle);
            HFunc(TEvConsole::TEvGetOperationResponse, Handle);
            HFunc(TEvConsole::TEvGetTenantStatusResponse, Handle);
            HFunc(TEvConsole::TEvListConfigValidatorsResponse, Handle);
            HFunc(TEvConsole::TEvListTenantsResponse, Handle);
            HFunc(TEvConsole::TEvGetNodeConfigItemsResponse, Handle);
            HFunc(TEvConsole::TEvGetNodeConfigResponse, Handle);
            HFunc(TEvConsole::TEvRemoveTenantResponse, Handle);
            HFunc(TEvConsole::TEvSetConfigResponse, Handle);
            HFunc(TEvConsole::TEvToggleConfigValidatorResponse, Handle);
            CFunc(TEvTabletPipe::EvClientDestroyed, Undelivered);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        default:
            Y_ABORT("TConsoleRequestActor::MainState unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(),
                   ev->ToString().data());
        }
    }

    bool CheckAccessGetNodeConfig() const {
        const auto serializedToken = TBase::GetSerializedToken();
        // Empty serializedToken means token is not required. Checked in secure_request.h
        if (!serializedToken.empty() && !AppData()->RegisterDynamicNodeAllowedSIDs.empty()) {
            NACLib::TUserToken token(serializedToken);
            for (const auto& sid : AppData()->RegisterDynamicNodeAllowedSIDs) {
                if (token.IsExist(sid)) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

private:
    NKikimrClient::TConsoleRequest Request;
    NKikimrClient::TConsoleResponse Response;
    ui32 StateStorageGroup = 0;
    TActorId ConsolePipe;
};

} // namespace

IActor* CreateMessageBusConsoleRequest(TBusMessageContext &msg)
{
    NKikimrClient::TConsoleRequest &record
        = static_cast<TBusConsoleRequest*>(msg.GetMessage())->Record;
    return new TConsoleRequestActor(record, msg);
}

} // namespace NMsgBusProxy
} // namespace NKikimr
