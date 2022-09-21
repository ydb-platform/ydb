#include "private_grpc.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_analytics_internal.h>
#include <ydb/library/protobuf_printer/security_printer.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYqPrivateTaskService::TGRpcYqPrivateTaskService(NActors::TActorSystem *system,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcYqPrivateTaskService::InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYqPrivateTaskService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcYqPrivateTaskService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYqPrivateTaskService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcYqPrivateTaskService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB)                                                                                  \
MakeIntrusive<TGRpcRequest<Yq::Private::NAME##Request, Yq::Private::NAME##Response, TGRpcYqPrivateTaskService, TSecurityTextFormatPrinter<Yq::Private::NAME##Request>, TSecurityTextFormatPrinter<Yq::Private::NAME##Response>>>( \
    this, &Service_, CQ_,                                                                                      \
    [this](NGrpc::IRequestContextBase *ctx) {                                                                  \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                       \
        ActorSystem_->Send(GRpcRequestProxyId_,                                                                \
            new TGrpcRequestOperationCall<Yq::Private::NAME##Request, Yq::Private::NAME##Response>                 \
                (ctx, &CB));                                                                                   \
    },                                                                                                         \
    &Yq::Private::V1::YqPrivateTaskService::AsyncService::Request##NAME,                                  \
    #NAME, logger, getCounterBlock("yql_internal", #NAME))                                                     \
    ->Run();                                                                                                   \

    ADD_REQUEST(PingTask, DoYqPrivatePingTaskRequest)

    ADD_REQUEST(GetTask, DoYqPrivateGetTaskRequest)

    ADD_REQUEST(WriteTaskResult, DoYqPrivateWriteTaskResultRequest)

    ADD_REQUEST(NodesHealthCheck, DoYqPrivateNodesHealthCheckRequest)

#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
