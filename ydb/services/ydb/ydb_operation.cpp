#include "ydb_operation.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcOperationService::TGRpcOperationService(NActors::TActorSystem *system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id) 
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id)
{ }

void TGRpcOperationService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) { 
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger)); 
}

void TGRpcOperationService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter *limiter) { 
    Limiter_ = limiter;
}

bool TGRpcOperationService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcOperationService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcOperationService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) { 
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Operations::IN, Ydb::Operations::OUT, TGRpcOperationService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \ 
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Operation::V1::OperationService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("operation", #NAME))->Run(); 

    ADD_REQUEST(GetOperation, GetOperationRequest, GetOperationResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvGetOperationRequest(ctx));
    })
    ADD_REQUEST(CancelOperation, CancelOperationRequest, CancelOperationResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCancelOperationRequest(ctx));
    })
    ADD_REQUEST(ForgetOperation, ForgetOperationRequest, ForgetOperationResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvForgetOperationRequest(ctx));
    })
    ADD_REQUEST(ListOperations, ListOperationsRequest, ListOperationsResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvListOperationsRequest(ctx));
    })
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
