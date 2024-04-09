#include "service_fq_internal.h"
#include "rpc_common/rpc_common.h"
#include "rpc_deferrable.h"

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>

#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/grpc_services/base/base.h>

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
using TEvFqPrivateCreateRateLimiterResourceRequest =
    TGrpcRequestOperationCall<Fq::Private::CreateRateLimiterResourceRequest, Fq::Private::CreateRateLimiterResourceResponse>;
using TEvFqPrivateDeleteRateLimiterResourceRequest =
    TGrpcRequestOperationCall<Fq::Private::DeleteRateLimiterResourceRequest, Fq::Private::DeleteRateLimiterResourceResponse>;

template <typename RpcRequestType, typename EvRequestType, typename EvResponseType>
class TFqPrivateRequestRPC : public TRpcOperationRequestActor<
    TFqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType>, RpcRequestType> {

    using TBase = TRpcOperationRequestActor<
        TFqPrivateRequestRPC<RpcRequestType,EvRequestType,EvResponseType>,
        RpcRequestType>;

public:
    TFqPrivateRequestRPC(IRequestOpCtx* request) : TBase(request) {}

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        const auto req = this->GetProtoRequest();
        auto ev = MakeHolder<EvRequestType>();
        auto request = dynamic_cast<RpcRequestType*>(this->Request_.get());
        Y_ABORT_UNLESS(request);
        auto proxyCtx = dynamic_cast<IRequestProxyCtx*>(request);
        Y_ABORT_UNLESS(proxyCtx);
        TString user;
        const TString& internalToken = proxyCtx->GetSerializedToken();
        if (internalToken) {
            NACLib::TUserToken userToken(internalToken);
            user = userToken.GetUserSID();
        }
        ev->Record = *req;
        ev->User = user;
        this->Send(NFq::MakeYqPrivateProxyId(), ev.Release());
        this->Become(&TFqPrivateRequestRPC<RpcRequestType, EvRequestType, EvResponseType>::StateFunc);
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
            req.SendResult(*ev->Get()->Record, ev->Get()->Status);
        }
    }
};

using TFqPrivatePingTaskRPC = TFqPrivateRequestRPC<
    TEvFqPrivatePingTaskRequest,
    NFq::TEvents::TEvPingTaskRequest,
    NFq::TEvents::TEvPingTaskResponse>;

using TFqPrivateGetTaskRPC = TFqPrivateRequestRPC<
    TEvFqPrivateGetTaskRequest,
    NFq::TEvents::TEvGetTaskRequest,
    NFq::TEvents::TEvGetTaskResponse>;

using TFqPrivateWriteTaskResultRPC = TFqPrivateRequestRPC<
    TEvFqPrivateWriteTaskResultRequest,
    NFq::TEvents::TEvWriteTaskResultRequest,
    NFq::TEvents::TEvWriteTaskResultResponse>;

using TFqPrivateNodesHealthCheckRPC = TFqPrivateRequestRPC<
    TEvFqPrivateNodesHealthCheckRequest,
    NFq::TEvents::TEvNodesHealthCheckRequest,
    NFq::TEvents::TEvNodesHealthCheckResponse>;

using TFqPrivateCreateRateLimiterResourceRPC = TFqPrivateRequestRPC<
    TEvFqPrivateCreateRateLimiterResourceRequest,
    NFq::TEvents::TEvCreateRateLimiterResourceRequest,
    NFq::TEvents::TEvCreateRateLimiterResourceResponse>;

using TFqPrivateDeleteRateLimiterResourceRPC = TFqPrivateRequestRPC<
    TEvFqPrivateDeleteRateLimiterResourceRequest,
    NFq::TEvents::TEvDeleteRateLimiterResourceRequest,
    NFq::TEvents::TEvDeleteRateLimiterResourceResponse>;

void DoFqPrivatePingTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivatePingTaskRPC(p.release()));
}

void DoFqPrivateGetTaskRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateGetTaskRPC(p.release()));
}

void DoFqPrivateWriteTaskResultRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateWriteTaskResultRPC(p.release()));
}

void DoFqPrivateNodesHealthCheckRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateNodesHealthCheckRPC(p.release()));
}

void DoFqPrivateCreateRateLimiterResourceRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateCreateRateLimiterResourceRPC(p.release()));
}

void DoFqPrivateDeleteRateLimiterResourceRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TFqPrivateDeleteRateLimiterResourceRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
