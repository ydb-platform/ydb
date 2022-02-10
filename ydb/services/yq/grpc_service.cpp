#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_yq.h>
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

    static const TVector<TString> CreateQueryPermissions = {
        "yq.queries.create",
        "yq.queries.invoke",
        "yq.connections.use",
        "yq.bindings.use",
        "yq.resources.managePublic"
    };
    static const TVector<TString> ListQueriesPermissions = {
        "yq.queries.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> DescribeQueryPermissions = {
        "yq.queries.get",
        "yq.queries.viewAst",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> GetQueryStatusPermissions = {
        "yq.queries.getStatus",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> ModifyQueryPermissions = {
        "yq.queries.update",
        "yq.queries.invoke",
        "yq.connections.use",
        "yq.bindings.use",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    static const TVector<TString> DeleteQueryPermissions = {
        "yq.queries.delete",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    static const TVector<TString> ControlQueryPermissions = {
        "yq.queries.control",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    static const TVector<TString> GetResultDataPermissions = {
        "yq.queries.getData",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> ListJobsPermissions = {
        "yq.jobs.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> DescribeJobPermissions = {
        "yq.jobs.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> CreateConnectionPermissions = {
        "yq.connections.create",
        "yq.resources.managePublic",
    };
    static const TVector<TString> ListConnectionsPermissions = {
        "yq.connections.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> DescribeConnectionPermissions = {
        "yq.connections.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> ModifyConnectionPermissions = {
        "yq.connections.update",
        "yq.resources.managePublic",
        "yq.resources.managePrivate",
    };
    static const TVector<TString> DeleteConnectionPermissions = {
        "yq.connections.delete",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    static const TVector<TString> TestConnectionPermissions = {
        "yq.connections.create",
    };
    static const TVector<TString> CreateBindingPermissions = {
        "yq.bindings.create",
        "yq.resources.managePublic"
    };
    static const TVector<TString> ListBindingsPermissions = {
        "yq.bindings.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> DescribeBindingPermissions = {
        "yq.bindings.get",
        "yq.resources.viewPublic",
        "yq.resources.viewPrivate"
    };
    static const TVector<TString> ModifyBindingPermissions = {
        "yq.bindings.update",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };
    static const TVector<TString> DeleteBindingPermissions = {
        "yq.bindings.delete",
        "yq.resources.managePublic",
        "yq.resources.managePrivate"
    };

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, PERMISSIONS)                                                                                  \
MakeIntrusive<TGRpcRequest<YandexQuery::NAME##Request, YandexQuery::NAME##Response, TGRpcYandexQueryService, TSecurityTextFormatPrinter<YandexQuery::NAME##Request>, TSecurityTextFormatPrinter<YandexQuery::NAME##Response>>>( \
    this, &Service_, CQ_,                                                                                      \
    [this](NGrpc::IRequestContextBase *ctx) {                                                                  \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                       \
        ActorSystem_->Send(GRpcRequestProxyId_,                                                                \
            new TGrpcYqRequestOperationCall<YandexQuery::NAME##Request, YandexQuery::NAME##Response>                 \
                (ctx, &CB, PERMISSIONS));                                                                                   \
    },                                                                                                         \
    &YandexQuery::V1::YandexQueryService::AsyncService::Request##NAME,                                  \
    #NAME, logger, getCounterBlock("yq", #NAME))                                                     \
    ->Run();                                                                                                   \

    ADD_REQUEST(CreateQuery, DoYandexQueryCreateQueryRequest, CreateQueryPermissions)
    ADD_REQUEST(ListQueries, DoYandexQueryListQueriesRequest, ListQueriesPermissions)
    ADD_REQUEST(DescribeQuery, DoYandexQueryDescribeQueryRequest, DescribeQueryPermissions)
    ADD_REQUEST(GetQueryStatus, DoYandexQueryGetQueryStatusRequest, GetQueryStatusPermissions)
    ADD_REQUEST(ModifyQuery, DoYandexQueryModifyQueryRequest, ModifyQueryPermissions)
    ADD_REQUEST(DeleteQuery, DoYandexQueryDeleteQueryRequest, DeleteQueryPermissions)
    ADD_REQUEST(ControlQuery, DoYandexQueryControlQueryRequest, ControlQueryPermissions)
    ADD_REQUEST(GetResultData, DoGetResultDataRequest, GetResultDataPermissions)
    ADD_REQUEST(ListJobs, DoListJobsRequest, ListJobsPermissions)
    ADD_REQUEST(DescribeJob, DoDescribeJobRequest, DescribeJobPermissions)
    ADD_REQUEST(CreateConnection, DoCreateConnectionRequest, CreateConnectionPermissions)
    ADD_REQUEST(ListConnections, DoListConnectionsRequest, ListConnectionsPermissions)
    ADD_REQUEST(DescribeConnection, DoDescribeConnectionRequest, DescribeConnectionPermissions)
    ADD_REQUEST(ModifyConnection, DoModifyConnectionRequest, ModifyConnectionPermissions)
    ADD_REQUEST(DeleteConnection, DoDeleteConnectionRequest, DeleteConnectionPermissions)
    ADD_REQUEST(TestConnection, DoTestConnectionRequest, TestConnectionPermissions)
    ADD_REQUEST(CreateBinding, DoCreateBindingRequest, CreateBindingPermissions)
    ADD_REQUEST(ListBindings, DoListBindingsRequest, ListBindingsPermissions)
    ADD_REQUEST(DescribeBinding, DoDescribeBindingRequest, DescribeBindingPermissions)
    ADD_REQUEST(ModifyBinding, DoModifyBindingRequest, ModifyBindingPermissions)
    ADD_REQUEST(DeleteBinding, DoDeleteBindingRequest, DeleteBindingPermissions)

#undef ADD_REQUEST

}

} // namespace NKikimr::NGRpcService
