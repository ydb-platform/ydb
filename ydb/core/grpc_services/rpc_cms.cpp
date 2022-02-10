#include "grpc_request_proxy.h" 
 
#include "rpc_calls.h" 
#include "rpc_deferrable.h" 
 
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
 
namespace NKikimr { 
namespace NGRpcService { 
 
using namespace NActors; 
using namespace NConsole; 
using namespace Ydb;
 
template <typename TRequest, typename TCmsRequest, typename TCmsResponse> 
class TCmsRPC : public TRpcOperationRequestActor<TCmsRPC<TRequest, TCmsRequest, TCmsResponse>, TRequest> {
    using TThis = TCmsRPC<TRequest, TCmsRequest, TCmsResponse>; 
    using TBase = TRpcOperationRequestActor<TThis, TRequest>;
 
    TActorId CmsPipe;
 
public: 
    TCmsRPC(TRequest* msg) 
        : TBase(msg)
    { 
    } 
 
    void Bootstrap(const TActorContext &ctx) 
    { 
        TBase::Bootstrap(ctx);

        auto dinfo = AppData(ctx)->DomainsInfo; 
        auto domain = dinfo->Domains.begin()->second; 
        ui32 group = dinfo->GetDefaultStateStorageGroup(domain->DomainUid); 
 
        NTabletPipe::TClientConfig pipeConfig; 
        pipeConfig.RetryPolicy = {.RetryLimitCount = 10};
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, MakeConsoleID(group), pipeConfig); 
        CmsPipe = ctx.RegisterWithSameMailbox(pipe);
 
        SendRequest(ctx); 
 
        this->Become(&TThis::StateWork); 
    } 
 
private: 
    void Die(const TActorContext &ctx) 
    { 
        NTabletPipe::CloseClient(ctx, CmsPipe); 
        TBase::Die(ctx); 
    } 
 
    template<typename T>
    void HandleWithOperationParams(T& ev, const TActorContext& ctx)
    { 
        const auto& response = ev->Get()->Record.GetResponse();
        if (response.operation().ready() == false
            && this->GetProtoRequest()->operation_params().operation_mode() == Ydb::Operations::OperationParams::SYNC) {
            auto request = MakeHolder<TEvConsole::TEvNotifyOperationCompletionRequest>();
            request->Record.MutableRequest()->set_id(response.operation().id());
            request->Record.SetUserToken(this->Request_->GetInternalToken());

            NTabletPipe::SendData(ctx, CmsPipe, request.Release());
        } else {
            TProtoResponseHelper::SendProtoResponse(response, response.operation().status(), this->Request_);
            Die(ctx);
        }
    }

    void Handle(TEvConsole::TEvCreateTenantResponse::TPtr& ev, const TActorContext& ctx) {
        HandleWithOperationParams(ev, ctx);
    }

    void Handle(TEvConsole::TEvRemoveTenantResponse::TPtr& ev, const TActorContext& ctx) {
        HandleWithOperationParams(ev, ctx);
    }

    template<typename T>
    void Handle(T& ev, const TActorContext& ctx)
    {
        auto& response = ev->Get()->Record.GetResponse();
        TProtoResponseHelper::SendProtoResponse(response, response.operation().status(), this->Request_);
        Die(ctx); 
    } 
 
    void Handle(TEvConsole::TEvOperationCompletionNotification::TPtr& ev, const TActorContext& ctx)
    {
        this->Request_->SendOperation(ev->Get()->Record.GetResponse().operation());
        Die(ctx);
    }

    void Handle(TEvConsole::TEvNotifyOperationCompletionResponse::TPtr& ev, const TActorContext& ctx)
    {
        if (ev->Get()->Record.GetResponse().operation().ready() == true) {
            this->Request_->SendOperation(ev->Get()->Record.GetResponse().operation());
            Die(ctx);
        }
    }

    void Undelivered(const TActorContext &ctx) { 
        this->Request_->RaiseIssue(NYql::TIssue("CMS is unavailable"));
        this->Request_->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
        Die(ctx); 
    } 
 
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) noexcept 
    { 
        if (ev->Get()->Status != NKikimrProto::OK) 
            Undelivered(ctx); 
    } 
 
    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) { 
            HFunc(TCmsResponse, Handle); 
            CFunc(TEvTabletPipe::EvClientDestroyed, Undelivered); 
            HFunc(TEvTabletPipe::TEvClientConnected, Handle); 
            HFunc(TEvConsole::TEvOperationCompletionNotification, Handle);
            HFunc(TEvConsole::TEvNotifyOperationCompletionResponse, Handle);
            default: TBase::StateFuncBase(ev, ctx);
        } 
    } 
 
    void SendRequest(const TActorContext &ctx) 
    { 
        auto request = MakeHolder<TCmsRequest>(); 
        request->Record.MutableRequest()->CopyFrom(*this->GetProtoRequest());
        request->Record.SetUserToken(this->Request_->GetInternalToken()); 
        NTabletPipe::SendData(ctx, CmsPipe, request.Release()); 
    } 
}; 
 
void TGRpcRequestProxy::Handle(TEvCreateTenantRequest::TPtr& ev, const TActorContext& ctx) 
{ 
    ctx.Register(new TCmsRPC<TEvCreateTenantRequest, 
                             TEvConsole::TEvCreateTenantRequest, 
                             TEvConsole::TEvCreateTenantResponse>(ev->Release().Release())); 
} 
 
void TGRpcRequestProxy::Handle(TEvAlterTenantRequest::TPtr& ev, const TActorContext& ctx) 
{ 
    ctx.Register(new TCmsRPC<TEvAlterTenantRequest, 
                             TEvConsole::TEvAlterTenantRequest, 
                             TEvConsole::TEvAlterTenantResponse>(ev->Release().Release())); 
} 
 
void TGRpcRequestProxy::Handle(TEvGetTenantStatusRequest::TPtr& ev, const TActorContext& ctx) 
{ 
    ctx.Register(new TCmsRPC<TEvGetTenantStatusRequest, 
                             TEvConsole::TEvGetTenantStatusRequest, 
                             TEvConsole::TEvGetTenantStatusResponse>(ev->Release().Release())); 
} 
 
void TGRpcRequestProxy::Handle(TEvListTenantsRequest::TPtr& ev, const TActorContext& ctx) 
{ 
    ctx.Register(new TCmsRPC<TEvListTenantsRequest, 
                             TEvConsole::TEvListTenantsRequest, 
                             TEvConsole::TEvListTenantsResponse>(ev->Release().Release())); 
} 
 
void TGRpcRequestProxy::Handle(TEvRemoveTenantRequest::TPtr& ev, const TActorContext& ctx) 
{ 
    ctx.Register(new TCmsRPC<TEvRemoveTenantRequest, 
                             TEvConsole::TEvRemoveTenantRequest, 
                             TEvConsole::TEvRemoveTenantResponse>(ev->Release().Release())); 
} 
 
void TGRpcRequestProxy::Handle(TEvDescribeTenantOptionsRequest::TPtr& ev, const TActorContext& ctx) 
{ 
    ctx.Register(new TCmsRPC<TEvDescribeTenantOptionsRequest, 
                             TEvConsole::TEvDescribeTenantOptionsRequest, 
                             TEvConsole::TEvDescribeTenantOptionsResponse>(ev->Release().Release())); 
} 
 
} // namespace NGRpcService 
} // namespace NKikimr 
