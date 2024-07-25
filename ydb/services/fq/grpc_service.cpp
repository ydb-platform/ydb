#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/library/grpc/server/dbg_printer.h>

namespace NKikimr::NGRpcService {

TGRpcFederatedQueryService::TGRpcFederatedQueryService(NActors::TActorSystem *system,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcFederatedQueryService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcFederatedQueryService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcFederatedQueryService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcFederatedQueryService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcFederatedQueryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB)                                                                                  \
MakeIntrusive<TGRpcRequest<FederatedQuery::NAME##Request, FederatedQuery::NAME##Response, TGRpcFederatedQueryService, TSecurityTextFormatPrinter<FederatedQuery::NAME##Request>, TSecurityTextFormatPrinter<FederatedQuery::NAME##Response>>>( \
    this, &Service_, CQ_,                                                                                      \
    [this](NYdbGrpc::IRequestContextBase *ctx) {                                                                  \
        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                       \
        ActorSystem_->Send(GRpcRequestProxyId_, CreateFederatedQuery##NAME##RequestOperationCall(ctx).release());            \
    },                                                                                                         \
    &FederatedQuery::V1::FederatedQueryService::AsyncService::Request##NAME,                                  \
    #NAME, logger, getCounterBlock("fq", #NAME))                                                     \
    ->Run();                                                                                                   \

    ADD_REQUEST(CreateQuery, DoFederatedQueryCreateQueryRequest)
    ADD_REQUEST(ListQueries, DoFederatedQueryListQueriesRequest)
    ADD_REQUEST(DescribeQuery, DoFederatedQueryDescribeQueryRequest)
    ADD_REQUEST(GetQueryStatus, DoFederatedQueryGetQueryStatusRequest)
    ADD_REQUEST(ModifyQuery, DoFederatedQueryModifyQueryRequest)
    ADD_REQUEST(DeleteQuery, DoFederatedQueryDeleteQueryRequest)
    ADD_REQUEST(ControlQuery, DoFederatedQueryControlQueryRequest)
    ADD_REQUEST(GetResultData, DoFederatedQueryGetResultDataRequest)
    ADD_REQUEST(ListJobs, DoFederatedQueryListJobsRequest)
    ADD_REQUEST(DescribeJob, DoFederatedQueryDescribeJobRequest)
    ADD_REQUEST(CreateConnection, DoFederatedQueryCreateConnectionRequest)
    ADD_REQUEST(ListConnections, DoFederatedQueryListConnectionsRequest)
    ADD_REQUEST(DescribeConnection, DoFederatedQueryDescribeConnectionRequest)
    ADD_REQUEST(ModifyConnection, DoFederatedQueryModifyConnectionRequest)
    ADD_REQUEST(DeleteConnection, DoFederatedQueryDeleteConnectionRequest)
    ADD_REQUEST(TestConnection, DoFederatedQueryTestConnectionRequest)
    ADD_REQUEST(CreateBinding, DoFederatedQueryCreateBindingRequest)
    ADD_REQUEST(ListBindings, DoFederatedQueryListBindingsRequest)
    ADD_REQUEST(DescribeBinding, DoFederatedQueryDescribeBindingRequest)
    ADD_REQUEST(ModifyBinding, DoFederatedQueryModifyBindingRequest)
    ADD_REQUEST(DeleteBinding, DoFederatedQueryDeleteBindingRequest)

#undef ADD_REQUEST

}

} // namespace NKikimr::NGRpcService

namespace NYdbGrpc {

template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::CreateQueryRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::CreateQueryResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ListQueriesRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ListQueriesResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DescribeQueryRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::GetQueryStatusRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::GetQueryStatusResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ModifyQueryRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ModifyQueryResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DeleteQueryRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DeleteQueryResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ControlQueryRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ControlQueryResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::GetResultDataRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::GetResultDataResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ListJobsRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ListJobsResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DescribeJobRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DescribeJobResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::CreateConnectionRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::CreateConnectionResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DescribeConnectionRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DescribeConnectionResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ModifyConnectionRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ModifyConnectionResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DeleteConnectionRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DeleteConnectionResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::TestConnectionRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::TestConnectionResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::CreateBindingRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::CreateBindingResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ListBindingsRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ListBindingsResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DescribeBindingRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DescribeBindingResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ModifyBindingRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::ModifyBindingResponse>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DeleteBindingRequest>>(const NProtoBuf::Message& message, bool ok);
template TString MakeDbgMessageString<NKikimr::TSecurityTextFormatPrinter<FederatedQuery::DeleteBindingResponse>>(const NProtoBuf::Message& message, bool ok);

}
