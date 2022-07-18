#include "service_fq_internal.h"
#include "rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/actors/proxy_private.h>

#include <library/cpp/actors/core/hfunc.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/yq/libs/protos/fq_private.pb.h>

namespace NKikimr {
namespace NGRpcService {

using TEvFqPrivatePingTaskRequest =
    TGrpcRequestOperationCall<Fq::Private::PingTaskRequest, Fq::Private::PingTaskResponse>;
using TEvFqPrivateGetTaskRequest =
    TGrpcRequestOperationCall<Fq::Private::GetTaskRequest, Fq::Private::GetTaskResponse>;
using TEvFqPrivateWriteTaskResultRequest =
    TGrpcRequestOperationCall<Fq::Private::WriteTaskResultRequest, Fq::Private::WriteTaskResultResponse>;
using TEvFqPrivateNodesHealthCheckRequest =
    TGrpcRequestOperationCall<Fq::Private::NodesHealthCheckRequest, Fq::Private::NodesHealthCheckResponse>;

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType, typename CastRequest, typename CastResult>
class TFqPrivateRequestRPC : public TRpcOperationRequestActor<
    TFqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType,CastRequest,CastResult>, RpcRequestType> {

    using TBase = TRpcOperationRequestActor<
        TFqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType,CastRequest,CastResult>,
        RpcRequestType>;

public:
    TFqPrivateRequestRPC(IRequestOpCtx* request) : TBase(request) {}

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        const auto req = this->GetProtoRequest();
        TProtoStringType protoString;
        Y_ENSURE(req->SerializeToString(&protoString));
        auto castedRequest = CastRequest();
        Y_ENSURE(castedRequest.ParseFromString(protoString));
        auto ev = MakeHolder<EvRequestType>();
        auto request = dynamic_cast<RpcRequestType*>(this->Request_.get());
        Y_VERIFY(request);
        auto proxyCtx = dynamic_cast<IRequestProxyCtx*>(request);
        Y_VERIFY(proxyCtx);
        TString user;
        const TString& internalToken = proxyCtx->GetInternalToken();
        if (internalToken) {
            NACLib::TUserToken userToken(internalToken);
            user = userToken.GetUserSID();
        }
        ev->Record = castedRequest;
        ev->User = user;
        this->Send(NYq::MakeYqPrivateProxyId(), ev.Release());
        this->Become(&TFqPrivateRequestRPC<RpcRequestType, EvRequestType, EvResponseType, CastRequest, CastResult>::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        HFunc(EvResponseType, Handle);
    )

    void Handle(typename EvResponseType::TPtr& ev, const TActorContext& ctx) {
        SendResponse(ev, *this->Request_);
        this->Die(ctx);
    }

    template <typename TEv, typename TReq>
    void SendResponse(const TEv& ev, TReq& req) {
        if (!ev->Get()->Record) {
            req.RaiseIssues(ev->Get()->Issues);
            req.ReplyWithYdbStatus(ev->Get()->Status);
        } else {
            TProtoStringType protoString;
            Y_ENSURE(ev->Get()->Record->SerializeToString(&protoString));
            auto castedResult = CastResult();
            Y_ENSURE(castedResult.ParseFromString(protoString));

            req.SendResult(castedResult, ev->Get()->Status);
        }
    }
};

using TFqPrivatePingTaskRPC = TFqPrivateRequestRPC<
    TEvFqPrivatePingTaskRequest,
    NYq::TEvents::TEvPingTaskRequest,
    NYq::TEvents::TEvPingTaskResponse,
    Yq::Private::PingTaskRequest,
    Fq::Private::PingTaskResult>;

using TFqPrivateGetTaskRPC = TFqPrivateRequestRPC<
    TEvFqPrivateGetTaskRequest,
    NYq::TEvents::TEvGetTaskRequest,
    NYq::TEvents::TEvGetTaskResponse,
    Yq::Private::GetTaskRequest,
    Fq::Private::GetTaskResult>;

using TFqPrivateWriteTaskResultRPC = TFqPrivateRequestRPC<
    TEvFqPrivateWriteTaskResultRequest,
    NYq::TEvents::TEvWriteTaskResultRequest,
    NYq::TEvents::TEvWriteTaskResultResponse,
    Yq::Private::WriteTaskResultRequest,
    Fq::Private::WriteTaskResultResult>;

using TFqPrivateNodesHealthCheckRPC = TFqPrivateRequestRPC<
    TEvFqPrivateNodesHealthCheckRequest,
    NYq::TEvents::TEvNodesHealthCheckRequest,
    NYq::TEvents::TEvNodesHealthCheckResponse,
    Yq::Private::NodesHealthCheckRequest,
    Fq::Private::NodesHealthCheckResult>;

void DoFqPrivatePingTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFqPrivatePingTaskRPC(p.release()));
}

void DoFqPrivateGetTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFqPrivateGetTaskRPC(p.release()));
}

void DoFqPrivateWriteTaskResultRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFqPrivateWriteTaskResultRPC(p.release()));
}

void DoFqPrivateNodesHealthCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TFqPrivateNodesHealthCheckRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
