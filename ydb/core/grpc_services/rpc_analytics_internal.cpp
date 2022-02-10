#include "service_analytics_internal.h"
#include "rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/actors/proxy_private.h>

#include <library/cpp/actors/core/hfunc.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/draft/yq_private.pb.h>

namespace NKikimr {
namespace NGRpcService {

using TEvYqPrivatePingTaskRequest =
    TGrpcRequestOperationCall<Yq::Private::PingTaskRequest, Yq::Private::PingTaskResponse>;
using TEvYqPrivateGetTaskRequest =
    TGrpcRequestOperationCall<Yq::Private::GetTaskRequest, Yq::Private::GetTaskResponse>;
using TEvYqPrivateWriteTaskResultRequest =
    TGrpcRequestOperationCall<Yq::Private::WriteTaskResultRequest, Yq::Private::WriteTaskResultResponse>;
using TEvYqPrivateNodesHealthCheckRequest =
    TGrpcRequestOperationCall<Yq::Private::NodesHealthCheckRequest, Yq::Private::NodesHealthCheckResponse>;

namespace {
    template <typename TEv, typename TReq>
    void SendResponse(const TEv& ev, TReq& req) {
        if (!ev->Get()->Record) {
            req.RaiseIssues(ev->Get()->Issues);
            req.ReplyWithYdbStatus(ev->Get()->Status);
        } else {
            req.SendResult(*ev->Get()->Record, ev->Get()->Status);
        }
    }

}

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType>
class TYqPrivateRequestRPC : public TRpcOperationRequestActor<
    TYqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType>, RpcRequestType> {

    using TBase = TRpcOperationRequestActor<
        TYqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType>,
        RpcRequestType>;

public:
    TYqPrivateRequestRPC(IRequestOpCtx* request) : TBase(request) {}

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        const auto req = this->GetProtoRequest();
        auto ev = MakeHolder<EvRequestType>();
        auto request = dynamic_cast<RpcRequestType*>(this->Request_.get());
        Y_VERIFY(request);
        ev->Record = *req;
        this->Send(NYq::MakeYqPrivateProxyId(), ev.Release());
        this->Become(&TYqPrivateRequestRPC<RpcRequestType, EvRequestType, EvResponseType>::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        HFunc(EvResponseType, Handle);
    )

    void Handle(typename EvResponseType::TPtr& ev, const TActorContext& ctx) {
        SendResponse(ev, *this->Request_);
        this->Die(ctx);
    }
};

using TYqPrivatePingTaskRPC = TYqPrivateRequestRPC<
    TEvYqPrivatePingTaskRequest,
    NYq::TEvents::TEvPingTaskRequest,
    NYq::TEvents::TEvPingTaskResponse>;

using TYqPrivateGetTaskRPC = TYqPrivateRequestRPC<
    TEvYqPrivateGetTaskRequest,
    NYq::TEvents::TEvGetTaskRequest,
    NYq::TEvents::TEvGetTaskResponse>;

using TYqPrivateWriteTaskResultRPC = TYqPrivateRequestRPC<
    TEvYqPrivateWriteTaskResultRequest,
    NYq::TEvents::TEvWriteTaskResultRequest,
    NYq::TEvents::TEvWriteTaskResultResponse>;

using TYqPrivateNodesHealthCheckRPC = TYqPrivateRequestRPC<
    TEvYqPrivateNodesHealthCheckRequest,
    NYq::TEvents::TEvNodesHealthCheckRequest,
    NYq::TEvents::TEvNodesHealthCheckResponse>;

void DoYqPrivatePingTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYqPrivatePingTaskRPC(p.release()));
}

void DoYqPrivateGetTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYqPrivateGetTaskRPC(p.release()));
}

void DoYqPrivateWriteTaskResultRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYqPrivateWriteTaskResultRPC(p.release()));
}

void DoYqPrivateNodesHealthCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TYqPrivateNodesHealthCheckRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
