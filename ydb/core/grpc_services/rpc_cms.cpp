#include "service_cms.h"
#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NConsole;
using namespace Ydb;

using TEvCreateTenantRequest = TGrpcRequestOperationCall<Cms::CreateDatabaseRequest,
    Cms::CreateDatabaseResponse>;
using TEvAlterTenantRequest = TGrpcRequestOperationCall<Cms::AlterDatabaseRequest,
    Cms::AlterDatabaseResponse>;
using TEvGetTenantStatusRequest = TGrpcRequestOperationCall<Cms::GetDatabaseStatusRequest,
    Cms::GetDatabaseStatusResponse>;
using TEvListTenantsRequest = TGrpcRequestOperationCall<Cms::ListDatabasesRequest,
    Cms::ListDatabasesResponse>;
using TEvRemoveTenantRequest = TGrpcRequestOperationCall<Cms::RemoveDatabaseRequest,
    Cms::RemoveDatabaseResponse>;
using TEvDescribeTenantOptionsRequest = TGrpcRequestOperationCall<Cms::DescribeDatabaseOptionsRequest,
    Cms::DescribeDatabaseOptionsResponse>;

template <typename TRequest, typename TCmsRequest, typename TCmsResponse>
class TCmsRPC : public TRpcOperationRequestActor<TCmsRPC<TRequest, TCmsRequest, TCmsResponse>, TRequest> {
    using TThis = TCmsRPC<TRequest, TCmsRequest, TCmsResponse>;
    using TBase = TRpcOperationRequestActor<TThis, TRequest>;

    TActorId CmsPipe;

public:
    TCmsRPC(IRequestOpCtx* msg)
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
            request->Record.SetUserToken(this->Request_->GetSerializedToken());

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
        request->Record.SetUserToken(this->Request_->GetSerializedToken());
        NTabletPipe::SendData(ctx, CmsPipe, request.Release());
    }
};

void DoCreateTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TCmsRPC<TEvCreateTenantRequest,
                    TEvConsole::TEvCreateTenantRequest,
                    TEvConsole::TEvCreateTenantResponse>(p.release()));
}

void DoAlterTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TCmsRPC<TEvAlterTenantRequest,
                    TEvConsole::TEvAlterTenantRequest,
                    TEvConsole::TEvAlterTenantResponse>(p.release()));
}

void DoGetTenantStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TCmsRPC<TEvGetTenantStatusRequest,
                    TEvConsole::TEvGetTenantStatusRequest,
                    TEvConsole::TEvGetTenantStatusResponse>(p.release()));
}

void DoListTenantsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TCmsRPC<TEvListTenantsRequest,
                TEvConsole::TEvListTenantsRequest,
                TEvConsole::TEvListTenantsResponse>(p.release()));
}

void DoRemoveTenantRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TCmsRPC<TEvRemoveTenantRequest,
                TEvConsole::TEvRemoveTenantRequest,
                TEvConsole::TEvRemoveTenantResponse>(p.release()));
}

void DoDescribeTenantOptionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TCmsRPC<TEvDescribeTenantOptionsRequest,
                    TEvConsole::TEvDescribeTenantOptionsRequest,
                    TEvConsole::TEvDescribeTenantOptionsResponse>(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
