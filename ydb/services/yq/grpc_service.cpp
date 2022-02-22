#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/library/protobuf_printer/security_printer.h>

namespace NKikimr::NGRpcService {

TGRpcYandexQueryService::TGRpcYandexQueryService(NActors::TActorSystem *system,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
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
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<YandexQuery::IN, YandexQuery::OUT, TGRpcYandexQueryService, TSecurityTextFormatPrinter<YandexQuery::IN>, TSecurityTextFormatPrinter<YandexQuery::OUT>>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &YandexQuery::V1::YandexQueryService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("yq", #NAME))->Run();

    ADD_REQUEST(CreateQuery, CreateQueryRequest, CreateQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryCreateQueryRequest(ctx));
    })

    ADD_REQUEST(ListQueries, ListQueriesRequest, ListQueriesResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryListQueriesRequest(ctx));
    })

    ADD_REQUEST(DescribeQuery, DescribeQueryRequest, DescribeQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryDescribeQueryRequest(ctx));
    })

    ADD_REQUEST(GetQueryStatus, GetQueryStatusRequest, GetQueryStatusResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryGetQueryStatusRequest(ctx));
    })

    ADD_REQUEST(ModifyQuery, ModifyQueryRequest, ModifyQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryModifyQueryRequest(ctx));
    })

    ADD_REQUEST(DeleteQuery, DeleteQueryRequest, DeleteQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryDeleteQueryRequest(ctx));
    })

    ADD_REQUEST(ControlQuery, ControlQueryRequest, ControlQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryControlQueryRequest(ctx));
    })

    ADD_REQUEST(GetResultData, GetResultDataRequest, GetResultDataResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryGetResultDataRequest(ctx));
    })

    ADD_REQUEST(ListJobs, ListJobsRequest, ListJobsResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryListJobsRequest(ctx));
    })

    ADD_REQUEST(DescribeJob, DescribeJobRequest, DescribeJobResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryDescribeJobRequest(ctx));
    })

    ADD_REQUEST(CreateConnection, CreateConnectionRequest, CreateConnectionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryCreateConnectionRequest(ctx));
    })

    ADD_REQUEST(ListConnections, ListConnectionsRequest, ListConnectionsResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryListConnectionsRequest(ctx));
    })

    ADD_REQUEST(DescribeConnection, DescribeConnectionRequest, DescribeConnectionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryDescribeConnectionRequest(ctx));
    })

    ADD_REQUEST(ModifyConnection, ModifyConnectionRequest, ModifyConnectionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryModifyConnectionRequest(ctx));
    })

    ADD_REQUEST(DeleteConnection, DeleteConnectionRequest, DeleteConnectionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryDeleteConnectionRequest(ctx));
    })

    ADD_REQUEST(TestConnection, TestConnectionRequest, TestConnectionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryTestConnectionRequest(ctx));
    })

    ADD_REQUEST(CreateBinding, CreateBindingRequest, CreateBindingResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryCreateBindingRequest(ctx));
    })

    ADD_REQUEST(ListBindings, ListBindingsRequest, ListBindingsResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryListBindingsRequest(ctx));
    })

    ADD_REQUEST(DescribeBinding, DescribeBindingRequest, DescribeBindingResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryDescribeBindingRequest(ctx));
    })

    ADD_REQUEST(ModifyBinding, ModifyBindingRequest, ModifyBindingResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryModifyBindingRequest(ctx));
    })

    ADD_REQUEST(DeleteBinding, DeleteBindingRequest, DeleteBindingResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvYandexQueryDeleteBindingRequest(ctx));
    })

#undef ADD_REQUEST
}

} // namespace NKikimr::NGRpcService
