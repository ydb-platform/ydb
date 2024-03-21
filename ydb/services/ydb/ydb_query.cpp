#include "ydb_query.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/query/service_query.h>

namespace NKikimr::NGRpcService {

TGRpcYdbQueryService::TGRpcYdbQueryService(NActors::TActorSystem *system,
                                           TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                           const NActors::TActorId& proxyId,
                                           bool rlAllowed,
                                           size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxyId, rlAllowed)
    , HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue))
{
}

TGRpcYdbQueryService::TGRpcYdbQueryService(NActors::TActorSystem *system,
                                           TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                           const TVector<NActors::TActorId>& proxies,
                                           bool rlAllowed,
                                           size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxies, rlAllowed)
    , HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue))
{
}

void TGRpcYdbQueryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Query;
    using namespace NQuery;

    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    size_t proxyCounter = 0;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB, REQUEST_TYPE, ...) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {  \
        for (auto* cq: CQS) { \
            MakeIntrusive<TGRpcRequest<IN, OUT, TGRpcYdbQueryService>>(this, &Service_, cq, \
                [this, proxyCounter](NYdbGrpc::IRequestContextBase* ctx) { \
                    NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
                    ActorSystem_->Send(GRpcProxies_[proxyCounter % GRpcProxies_.size()], \
                        new TGrpcRequestNoOperationCall<IN, OUT> \
                            (ctx, &CB, TRequestAuxSettings { \
                                .RlMode = RLSWITCH(TRateLimiterMode::Rps), \
                                __VA_OPT__(.AuditMode = TAuditMode::__VA_ARGS__,) \
                                .RequestType = NJaegerTracing::ERequestType::QUERY_##REQUEST_TYPE, \
                            })); \
                }, &Ydb::Query::V1::QueryService::AsyncService::Request ## NAME, \
                #NAME, logger, getCounterBlock("query", #NAME))->Run(); \
            ++proxyCounter; \
        }  \
    }

    ADD_REQUEST(ExecuteQuery, ExecuteQueryRequest, ExecuteQueryResponsePart, DoExecuteQuery, EXECUTEQUERY, Auditable);
    ADD_REQUEST(ExecuteScript, ExecuteScriptRequest, Ydb::Operations::Operation, DoExecuteScript, EXECUTESCRIPT, Auditable);
    ADD_REQUEST(FetchScriptResults, FetchScriptResultsRequest, FetchScriptResultsResponse, DoFetchScriptResults, FETCHSCRIPTRESULTS);
    ADD_REQUEST(CreateSession, CreateSessionRequest, CreateSessionResponse, DoCreateSession, CREATESESSION);
    ADD_REQUEST(DeleteSession, DeleteSessionRequest, DeleteSessionResponse, DoDeleteSession, DELETESESSION);
    ADD_REQUEST(AttachSession, AttachSessionRequest, SessionState, DoAttachSession, ATTACHSESSION);
    ADD_REQUEST(BeginTransaction, BeginTransactionRequest, BeginTransactionResponse, DoBeginTransaction, BEGINTRANSACTION);
    ADD_REQUEST(CommitTransaction, CommitTransactionRequest, CommitTransactionResponse, DoCommitTransaction, COMMITTRANSACTION);
    ADD_REQUEST(RollbackTransaction, RollbackTransactionRequest, RollbackTransactionResponse, DoRollbackTransaction, ROLLBACKTRANSACTION);

#undef ADD_REQUEST
}

} // namespace NKikimr::NGRpcService
