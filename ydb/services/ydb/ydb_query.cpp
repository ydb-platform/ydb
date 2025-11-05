#include "ydb_query.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/query/service_query.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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

#ifdef SETUP_QUERY_METHOD
#error SETUP_QUERY_METHOD macro already defined
#endif

#define SETUP_QUERY_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                          \
        for (auto* cq: CQS) {                                                          \
            SETUP_RUNTIME_EVENT_METHOD(                                                \
                methodName,                                                            \
                inputType,                                                             \
                outputType,                                                            \
                methodCallback,                                                        \
                rlMode,                                                                \
                requestType,                                                           \
                YDB_API_DEFAULT_COUNTER_BLOCK(query, methodName),                      \
                auditMode,                                                             \
                COMMON,                                                                \
                ::NKikimr::NGRpcService::TGrpcRequestNoOperationCall,                  \
                GRpcProxies_[proxyCounter % GRpcProxies_.size()],                      \
                cq,                                                                    \
                nullptr,                                                               \
                nullptr                                                                \
            );                                                                         \
            ++proxyCounter;                                                            \
        }                                                                              \
    }

    SETUP_QUERY_METHOD(ExecuteQuery, ExecuteQueryRequest, ExecuteQueryResponsePart, DoExecuteQuery, RLSWITCH(Rps), QUERY_EXECUTEQUERY, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_QUERY_METHOD(ExecuteScript, ExecuteScriptRequest, Ydb::Operations::Operation, DoExecuteScript, RLSWITCH(Rps), QUERY_EXECUTESCRIPT, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_QUERY_METHOD(FetchScriptResults, FetchScriptResultsRequest, FetchScriptResultsResponse, DoFetchScriptResults, RLSWITCH(Rps), QUERY_FETCHSCRIPTRESULTS, TAuditMode::NonModifying());
    SETUP_QUERY_METHOD(CreateSession, CreateSessionRequest, CreateSessionResponse, DoCreateSession, RLSWITCH(Rps), QUERY_CREATESESSION, TAuditMode::NonModifying());
    SETUP_QUERY_METHOD(DeleteSession, DeleteSessionRequest, DeleteSessionResponse, DoDeleteSession, RLSWITCH(Rps), QUERY_DELETESESSION, TAuditMode::NonModifying());
    SETUP_QUERY_METHOD(AttachSession, AttachSessionRequest, SessionState, DoAttachSession, RLSWITCH(Rps), QUERY_ATTACHSESSION, TAuditMode::NonModifying());
    SETUP_QUERY_METHOD(BeginTransaction, BeginTransactionRequest, BeginTransactionResponse, DoBeginTransaction, RLSWITCH(Rps), QUERY_BEGINTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_QUERY_METHOD(CommitTransaction, CommitTransactionRequest, CommitTransactionResponse, DoCommitTransaction, RLSWITCH(Rps), QUERY_COMMITTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_QUERY_METHOD(RollbackTransaction, RollbackTransactionRequest, RollbackTransactionResponse, DoRollbackTransaction, RLSWITCH(Rps), QUERY_ROLLBACKTRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));

#undef SETUP_QUERY_METHOD
}

} // namespace NKikimr::NGRpcService
