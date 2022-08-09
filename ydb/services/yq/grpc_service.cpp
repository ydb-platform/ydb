#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_yq.h>
#include <ydb/library/protobuf_printer/security_printer.h>

namespace NKikimr::NGRpcService {

TGRpcYandexQueryService::TGRpcYandexQueryService(NActors::TActorSystem *system,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcYandexQueryService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYandexQueryService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcYandexQueryService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYandexQueryService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcYandexQueryService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB)                                                                                  \
MakeIntrusive<TGRpcRequest<YandexQuery::NAME##Request, YandexQuery::NAME##Response, TGRpcYandexQueryService, TSecurityTextFormatPrinter<YandexQuery::NAME##Request>, TSecurityTextFormatPrinter<YandexQuery::NAME##Response>>>( \
    this, &Service_, CQ_,                                                                                      \
    [this](NGrpc::IRequestContextBase *ctx) {                                                                  \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                       \
        ActorSystem_->Send(GRpcRequestProxyId_, Create##NAME##RequestOperationCall(ctx).release());            \
    },                                                                                                         \
    &YandexQuery::V1::YandexQueryService::AsyncService::Request##NAME,                                         \
    #NAME, logger, getCounterBlock("yq", #NAME))                                                               \
    ->Run();                                                                                                   \

    ADD_REQUEST(CreateQuery, DoYandexQueryCreateQueryRequest)
    ADD_REQUEST(ListQueries, DoYandexQueryListQueriesRequest)
    ADD_REQUEST(DescribeQuery, DoYandexQueryDescribeQueryRequest)
    ADD_REQUEST(GetQueryStatus, DoYandexQueryGetQueryStatusRequest)
    ADD_REQUEST(ModifyQuery, DoYandexQueryModifyQueryRequest)
    ADD_REQUEST(DeleteQuery, DoYandexQueryDeleteQueryRequest)
    ADD_REQUEST(ControlQuery, DoYandexQueryControlQueryRequest)
    ADD_REQUEST(GetResultData, DoYandexQueryGetResultDataRequest)
    ADD_REQUEST(ListJobs, DoYandexQueryListJobsRequest)
    ADD_REQUEST(DescribeJob, DoYandexQueryDescribeJobRequest)
    ADD_REQUEST(CreateConnection, DoYandexQueryCreateConnectionRequest)
    ADD_REQUEST(ListConnections, DoYandexQueryListConnectionsRequest)
    ADD_REQUEST(DescribeConnection, DoYandexQueryDescribeConnectionRequest)
    ADD_REQUEST(ModifyConnection, DoYandexQueryModifyConnectionRequest)
    ADD_REQUEST(DeleteConnection, DoYandexQueryDeleteConnectionRequest)
    ADD_REQUEST(TestConnection, DoYandexQueryTestConnectionRequest)
    ADD_REQUEST(CreateBinding, DoYandexQueryCreateBindingRequest)
    ADD_REQUEST(ListBindings, DoYandexQueryListBindingsRequest)
    ADD_REQUEST(DescribeBinding, DoYandexQueryDescribeBindingRequest)
    ADD_REQUEST(ModifyBinding, DoYandexQueryModifyBindingRequest)
    ADD_REQUEST(DeleteBinding, DoYandexQueryDeleteBindingRequest)

#undef ADD_REQUEST

}

} // namespace NKikimr::NGRpcService
