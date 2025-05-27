#include "msgbus_server.h"
#include "msgbus_server_proxy.h"
#include "msgbus_server_request.h"
#include "msgbus_securereq.h"
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/ticket_parser.h>

namespace NKikimr {
namespace NMsgBusProxy {

using namespace NSchemeShard;

class TMessageBusSchemeInitRoot : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusSchemeInitRoot>> {
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusSchemeInitRoot>>;
    THolder<TBusSchemeInitRoot> Request;
    const bool WithRetry = true;
    TActorId PipeClient;

    void ReplyWithResult(EResponseStatus status, TEvSchemeShard::TEvInitRootShardResult::EStatus ssStatus, const TActorContext &ctx) {
        TAutoPtr<TBusResponseStatus> response(new TBusResponseStatus(status));
        response->Record.SetSchemeStatus(ssStatus);
        SendReplyAutoPtr(response);
        Request.Destroy();
        Die(ctx);
    }

    void Handle(TEvSchemeShard::TEvInitRootShardResult::TPtr& ev, const TActorContext& ctx) {
        const NKikimrTxScheme::TEvInitRootShardResult &record = ev->Get()->Record;
        const auto status = (TEvSchemeShard::TEvInitRootShardResult::EStatus)record.GetStatus();
        switch (status) {
        case TEvSchemeShard::TEvInitRootShardResult::StatusSuccess:
        case TEvSchemeShard::TEvInitRootShardResult::StatusAlreadyInitialized:
            return ReplyWithResult(MSTATUS_OK, status, ctx);
        default:
            return ReplyWithResult(MSTATUS_ERROR, status, ctx);
        }
    }

    void Die(const TActorContext &ctx) override {
        if (PipeClient) {
            NTabletPipe::CloseClient(ctx, PipeClient);
            PipeClient = TActorId();
        }
        TBase::Die(ctx);
    }

public:
    TMessageBusSchemeInitRoot(TEvBusProxy::TEvInitRoot *msg)
        : TBase(msg->MsgContext)
        , Request(static_cast<TBusSchemeInitRoot*>(msg->MsgContext.ReleaseMessage()))
    {
        TBase::SetSecurityToken(Request->Record.GetSecurityToken());
        TBase::SetRequireAdminAccess(true);
        TBase::SetPeerName(msg->MsgContext.GetPeerName());
    }

    void Bootstrap(const TActorContext &ctx) {
        SendRequest(ctx);
        Become(&TMessageBusSchemeInitRoot::StateWork);
    }

    //STFUNC(StateWork)
    void StateWork(TAutoPtr<NActors::IEventHandle> &ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSchemeShard::TEvInitRootShardResult, Handle);
        }
    }

    void SendRequest(const TActorContext &ctx) {
        TString tagName = Request->Record.GetTagName();
        const TDomainsInfo::TDomain *domain = AppData(ctx)->DomainsInfo->GetDomainByName(tagName);
        if (domain != nullptr) {
            THolder<TEvSchemeShard::TEvInitRootShard> x = MakeHolder<TEvSchemeShard::TEvInitRootShard>(ctx.SelfID, domain->DomainRootTag(), domain->Name);
            if (Request->Record.HasGlobalConfig()) {
                x->Record.MutableConfig()->MergeFrom(Request->Record.GetGlobalConfig());
            }
            x->Record.SetOwner(TBase::GetUserSID());
            x->Record.MutableStoragePools()->CopyFrom(Request->Record.GetStoragePools());
            NTabletPipe::TClientConfig clientConfig;
            if (WithRetry) {
                clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            }
            PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, domain->SchemeRoot, clientConfig));
            NTabletPipe::SendData(ctx, PipeClient, x.Release());
        } else {
            ReplyWithResult(MSTATUS_ERROR, TEvSchemeShard::TEvInitRootShardResult::StatusBadArgument, ctx);
        }
    }
};

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvInitRoot::TPtr& ev, const TActorContext& ctx) {
    TEvBusProxy::TEvInitRoot *msg = ev->Get();
    ctx.Register(new TMessageBusSchemeInitRoot(msg));
}

}
}
