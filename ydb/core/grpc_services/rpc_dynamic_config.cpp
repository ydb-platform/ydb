#include "service_dynamic_config.h"
#include "rpc_deferrable.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/public/api/protos/draft/ydb_dynamic_config.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace NConsole;
using namespace Ydb;

using TEvApplyConfigRequest = TGrpcRequestOperationCall<DynamicConfig::ApplyConfigRequest,
    DynamicConfig::ApplyConfigResponse>;

using TEvDropConfigRequest = TGrpcRequestOperationCall<DynamicConfig::DropConfigRequest,
    DynamicConfig::DropConfigResponse>;

using TEvAddVolatileConfigRequest = TGrpcRequestOperationCall<DynamicConfig::AddVolatileConfigRequest,
    DynamicConfig::AddVolatileConfigResponse>;

using TEvRemoveVolatileConfigRequest = TGrpcRequestOperationCall<DynamicConfig::RemoveVolatileConfigRequest,
    DynamicConfig::RemoveVolatileConfigResponse>;

using TEvGetConfigRequest = TGrpcRequestOperationCall<DynamicConfig::GetConfigRequest,
    DynamicConfig::GetConfigResponse>;

using TEvResolveConfigRequest = TGrpcRequestOperationCall<DynamicConfig::ResolveConfigRequest,
    DynamicConfig::ResolveConfigResponse>;

using TEvResolveAllConfigRequest = TGrpcRequestOperationCall<DynamicConfig::ResolveAllConfigRequest,
    DynamicConfig::ResolveAllConfigResponse>;

template <typename TRequest, typename TConsoleRequest, typename TConsoleResponse>
class TDynamicConfigRPC : public TRpcOperationRequestActor<TDynamicConfigRPC<TRequest, TConsoleRequest, TConsoleResponse>, TRequest> {
    using TThis = TDynamicConfigRPC<TRequest, TConsoleRequest, TConsoleResponse>;
    using TBase = TRpcOperationRequestActor<TThis, TRequest>;

    TActorId ConsolePipe;

public:
    TDynamicConfigRPC(IRequestOpCtx* msg)
        : TBase(msg)
    {
    }

    void Bootstrap()
    {
        TBase::Bootstrap(TActivationContext::AsActorContext());

        auto dinfo = AppData()->DomainsInfo;
        auto domain = dinfo->Domains.begin()->second;
        ui32 group = dinfo->GetDefaultStateStorageGroup(domain->DomainUid);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {
            .RetryLimitCount = 10,
        };
        auto pipe = NTabletPipe::CreateClient(IActor::SelfId(), MakeConsoleID(group), pipeConfig);
        ConsolePipe = IActor::RegisterWithSameMailbox(pipe);

        SendRequest();

        this->Become(&TThis::StateWork);
    }

private:
    void PassAway()
    {
        NTabletPipe::CloseClient(IActor::SelfId(), ConsolePipe);
        TBase::PassAway();
    }

    template<typename T>
    void HandleWithOperationParams(T& ev)
    {
        const auto& response = ev->Get()->Record.GetResponse();
        if (response.operation().ready() == false
            && this->GetProtoRequest()->operation_params().operation_mode() == Ydb::Operations::OperationParams::SYNC) {
            auto request = MakeHolder<TEvConsole::TEvNotifyOperationCompletionRequest>();
            request->Record.MutableRequest()->set_id(response.operation().id());
            request->Record.SetUserToken(this->Request_->GetSerializedToken());

            NTabletPipe::SendData(IActor::SelfId(), ConsolePipe, request.Release());
        } else {
            return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, response, TActivationContext::AsActorContext());
        }
    }

    void Handle(TEvConsole::TEvGetAllConfigsResponse::TPtr& ev)
    {
        HandleWithOperationParams(ev);
    }

    void Handle(TEvConsole::TEvResolveConfigResponse::TPtr& ev)
    {
        HandleWithOperationParams(ev);
    }

    void Handle(TEvConsole::TEvResolveAllConfigResponse::TPtr& ev)
    {
        HandleWithOperationParams(ev);
    }

    template<typename T>
    void Handle(T& ev)
    {
        auto& response = ev->Get()->Record.GetResponse();
        TProtoResponseHelper::SendProtoResponse(response, response.operation().status(), this->Request_);
        PassAway();
    }

    void Handle(TEvConsole::TEvOperationCompletionNotification::TPtr& ev)
    {
        this->Request_->SendOperation(ev->Get()->Record.GetResponse().operation());
        PassAway();
    }

    void Handle(TEvConsole::TEvNotifyOperationCompletionResponse::TPtr& ev)
    {
        if (ev->Get()->Record.GetResponse().operation().ready() == true) {
            this->Request_->SendOperation(ev->Get()->Record.GetResponse().operation());
            PassAway();
        }
    }

    void Undelivered()
    {
        this->Request_->RaiseIssue(NYql::TIssue("Console is unavailable"));
        this->Request_->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
        PassAway();
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev) noexcept
    {
        if (ev->Get()->Status != NKikimrProto::OK)
            Undelivered();
    }

    void StateWork(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            hFunc(TConsoleResponse, Handle);
            cFunc(TEvTabletPipe::EvClientDestroyed, Undelivered);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvConsole::TEvOperationCompletionNotification, Handle);
            hFunc(TEvConsole::TEvNotifyOperationCompletionResponse, Handle);
            default: TBase::StateFuncBase(ev);
        }
    }

    void SendRequest()
    {
        auto request = MakeHolder<TConsoleRequest>();
        request->Record.MutableRequest()->CopyFrom(*this->GetProtoRequest());
        request->Record.SetUserToken(this->Request_->GetSerializedToken());
        NTabletPipe::SendData(IActor::SelfId(), ConsolePipe, request.Release());
    }
};

void DoApplyConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvApplyConfigRequest,
                    TEvConsole::TEvApplyConfigRequest,
                    TEvConsole::TEvApplyConfigResponse>(p.release()));
}

void DoDropConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvDropConfigRequest,
                    TEvConsole::TEvDropConfigRequest,
                    TEvConsole::TEvDropConfigResponse>(p.release()));
}

void DoAddVolatileConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvAddVolatileConfigRequest,
                    TEvConsole::TEvAddVolatileConfigRequest,
                    TEvConsole::TEvAddVolatileConfigResponse>(p.release()));
}

void DoRemoveVolatileConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvRemoveVolatileConfigRequest,
                    TEvConsole::TEvRemoveVolatileConfigRequest,
                    TEvConsole::TEvRemoveVolatileConfigResponse>(p.release()));
}

void DoGetConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvGetConfigRequest,
                    TEvConsole::TEvGetAllConfigsRequest,
                    TEvConsole::TEvGetAllConfigsResponse>(p.release()));
}

void DoResolveConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvResolveConfigRequest,
                    TEvConsole::TEvResolveConfigRequest,
                    TEvConsole::TEvResolveConfigResponse>(p.release()));
}

void DoResolveAllConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvResolveAllConfigRequest,
                    TEvConsole::TEvResolveAllConfigRequest,
                    TEvConsole::TEvResolveAllConfigResponse>(p.release()));
}

} // namespace NKikimr::NGRpcService
