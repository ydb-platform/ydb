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

using TEvSetConfigRequest = TGrpcRequestOperationCall<DynamicConfig::SetConfigRequest,
    DynamicConfig::SetConfigResponse>;

using TEvReplaceConfigRequest = TGrpcRequestOperationCall<DynamicConfig::ReplaceConfigRequest,
    DynamicConfig::ReplaceConfigResponse>;

using TEvDropConfigRequest = TGrpcRequestOperationCall<DynamicConfig::DropConfigRequest,
    DynamicConfig::DropConfigResponse>;

using TEvAddVolatileConfigRequest = TGrpcRequestOperationCall<DynamicConfig::AddVolatileConfigRequest,
    DynamicConfig::AddVolatileConfigResponse>;

using TEvRemoveVolatileConfigRequest = TGrpcRequestOperationCall<DynamicConfig::RemoveVolatileConfigRequest,
    DynamicConfig::RemoveVolatileConfigResponse>;

using TEvGetNodeLabelsRequest = TGrpcRequestOperationCall<DynamicConfig::GetNodeLabelsRequest,
    DynamicConfig::GetNodeLabelsResponse>;

using TEvGetMetadataRequest = TGrpcRequestOperationCall<DynamicConfig::GetMetadataRequest,
    DynamicConfig::GetMetadataResponse>;

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

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {
            .RetryLimitCount = 10,
        };
        auto pipe = NTabletPipe::CreateClient(IActor::SelfId(), MakeConsoleID(), pipeConfig);
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
        } else if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            return TBase::Reply(response.operation().status(), response.operation().issues(), TActivationContext::AsActorContext());
        } else {
            return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, response, TActivationContext::AsActorContext());
        }
    }

    void Handle(TEvConsole::TEvGetAllMetadataResponse::TPtr& ev)
    {
        return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record.GetResponse(), TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvGetAllConfigsResponse::TPtr& ev)
    {
        return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record.GetResponse(), TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvResolveConfigResponse::TPtr& ev)
    {
        return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record.GetResponse(), TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvResolveAllConfigResponse::TPtr& ev)
    {
        return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record.GetResponse(), TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvGetNodeLabelsResponse::TPtr& ev) {
        return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, ev->Get()->Record.GetResponse(), TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvUnauthorized::TPtr&) {
        ::google::protobuf::RepeatedPtrField< ::Ydb::Issue::IssueMessage> issues;
        auto issue = issues.Add();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message("User must have administrator rights");

        return TBase::Reply(Ydb::StatusIds::UNAUTHORIZED, issues, TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvDisabled::TPtr&) {
        ::google::protobuf::RepeatedPtrField< ::Ydb::Issue::IssueMessage> issues;
        auto issue = issues.Add();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message("Feature is disabled");

        return TBase::Reply(Ydb::StatusIds::BAD_REQUEST, issues, TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvGenericError::TPtr& ev) {
        return TBase::Reply(ev->Get()->Record.GetYdbStatus(), ev->Get()->Record.GetIssues(), TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvSetYamlConfigResponse::TPtr& ev) {
        return TBase::Reply(Ydb::StatusIds::SUCCESS, ev->Get()->Record.GetIssues(), TActivationContext::AsActorContext());
    }

    void Handle(TEvConsole::TEvReplaceYamlConfigResponse::TPtr& ev) {
        return TBase::Reply(Ydb::StatusIds::SUCCESS, ev->Get()->Record.GetIssues(), TActivationContext::AsActorContext());
    }

    template<typename T>
    void Handle(T& ev)
    {
        Y_UNUSED(ev);
        TBase::Reply(Ydb::StatusIds::SUCCESS, TActivationContext::AsActorContext());
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
            hFunc(TEvConsole::TEvUnauthorized, Handle);
            hFunc(TEvConsole::TEvDisabled, Handle);
            hFunc(TEvConsole::TEvGenericError, Handle);
            default: TBase::StateFuncBase(ev);
        }
    }

    void SendRequest()
    {
        auto request = MakeHolder<TConsoleRequest>();
        request->Record.MutableRequest()->CopyFrom(*this->GetProtoRequest());
        request->Record.SetUserToken(this->Request_->GetSerializedToken());
        request->Record.SetPeerName(this->Request_->GetPeerName());
        NTabletPipe::SendData(IActor::SelfId(), ConsolePipe, request.Release());
    }
};

void DoSetConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvSetConfigRequest,
                    TEvConsole::TEvSetYamlConfigRequest,
                    TEvConsole::TEvSetYamlConfigResponse>(p.release()));
}

void DoReplaceConfigRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvReplaceConfigRequest,
                    TEvConsole::TEvReplaceYamlConfigRequest,
                    TEvConsole::TEvReplaceYamlConfigResponse>(p.release()));
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

void DoGetNodeLabelsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvGetNodeLabelsRequest,
                    TEvConsole::TEvGetNodeLabelsRequest,
                    TEvConsole::TEvGetNodeLabelsResponse>(p.release()));
}

void DoGetMetadataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(
        new TDynamicConfigRPC<TEvGetMetadataRequest,
                    TEvConsole::TEvGetAllMetadataRequest,
                    TEvConsole::TEvGetAllMetadataResponse>(p.release()));
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
