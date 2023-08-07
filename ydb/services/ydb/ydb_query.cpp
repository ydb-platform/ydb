#include "ydb_query.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/query/service_query.h>

namespace NKikimr::NGRpcService {

void TGRpcYdbQueryService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    using namespace Ydb::Query;
    using namespace NQuery;

    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<IN, OUT, TGRpcYdbQueryService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase* ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Query::V1::QueryService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("query", #NAME))->Run();

    ADD_REQUEST(ExecuteQuery, ExecuteQueryRequest, ExecuteQueryResponsePart, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<ExecuteQueryRequest, ExecuteQueryResponsePart>
                (ctx, &DoExecuteQuery, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));
    })

    ADD_REQUEST(ExecuteScript, ExecuteScriptRequest, Ydb::Operations::Operation, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<ExecuteScriptRequest, Ydb::Operations::Operation>
                (ctx, &DoExecuteScript, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));
    })

    ADD_REQUEST(FetchScriptResults, FetchScriptResultsRequest, FetchScriptResultsResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<FetchScriptResultsRequest, FetchScriptResultsResponse>
                (ctx, &DoFetchScriptResults, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));
    })

    ADD_REQUEST(CreateSession, CreateSessionRequest, CreateSessionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<CreateSessionRequest, CreateSessionResponse>
                (ctx, &DoCreateSession, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));
    })

    ADD_REQUEST(DeleteSession, DeleteSessionRequest, DeleteSessionResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<DeleteSessionRequest, DeleteSessionResponse>
                (ctx, &DoDeleteSession, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));
    })

    ADD_REQUEST(AttachSession, AttachSessionRequest, SessionState, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<AttachSessionRequest, SessionState>
                (ctx, &DoAttachSession, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));
    })

#undef ADD_REQUEST
}

} // namespace NKikimr::NGRpcService
