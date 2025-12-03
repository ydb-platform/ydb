#include "ydb_debug.h"

#include <ydb/core/grpc_services/service_debug.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TGRpcYdbDebugService::TGRpcYdbDebugService(NActors::TActorSystem *system,
                                         TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                         const NActors::TActorId& proxyId,
                                         bool rlAllowed,
                                         size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxyId, rlAllowed)
    , HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue))
{
}

TGRpcYdbDebugService::TGRpcYdbDebugService(NActors::TActorSystem *system,
                                         TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                         const TVector<NActors::TActorId>& proxies,
                                         bool rlAllowed,
                                         size_t handlersPerCompletionQueue)
    : TGrpcServiceBase(system, counters, proxies, rlAllowed)
    , HandlersPerCompletionQueue(Max(size_t{1}, handlersPerCompletionQueue))
{
}

void TGRpcYdbDebugService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Debug;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    size_t proxyCounter = 0;

    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {
        for (auto* cq: CQS) {
            MakeIntrusive<TGRpcRequest<PlainGrpcRequest, PlainGrpcResponse, TGRpcYdbDebugService>>(this, &Service_, cq,
                [this](NYdbGrpc::IRequestContextBase* reqCtx) {
                    NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer());
                    PlainGrpcResponse response;
                    auto ts = TInstant::Now();
                    response.SetCallBackTs(ts.MicroSeconds());
                    reqCtx->Reply(&response, 0);
                }, &TGrpcAsyncService::RequestPingPlainGrpc,
                "PingPlainGrpc", logger, YDB_API_DEFAULT_COUNTER_BLOCK(ping, PingPlainGrpc))->Run();
        }
    }

#ifdef SETUP_DEBUG_METHOD
#error SETUP_DEBUG_METHOD macro already defined
#endif

#define SETUP_DEBUG_METHOD(methodName, inputType, outputType, methodCallback, rlMode, requestType, auditMode) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                          \
        for (auto* cq: CQS) {                                                          \
            SETUP_RUNTIME_EVENT_METHOD(                                                \
                methodName,                                                            \
                inputType,                                                             \
                outputType,                                                            \
                methodCallback,                                                        \
                rlMode,                                                                \
                requestType,                                                           \
                YDB_API_DEFAULT_COUNTER_BLOCK(ping, methodName),                       \
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

    SETUP_DEBUG_METHOD(PingGrpcProxy, GrpcProxyRequest, GrpcProxyResponse, DoGrpcProxyPing, RLSWITCH(Rps), PING_PROXY, TAuditMode::NonModifying());
    SETUP_DEBUG_METHOD(PingKqpProxy, KqpProxyRequest, KqpProxyResponse, DoKqpPing, RLSWITCH(Rps), PING_KQP, TAuditMode::NonModifying());
    SETUP_DEBUG_METHOD(PingSchemeCache, SchemeCacheRequest, SchemeCacheResponse, DoSchemeCachePing, RLSWITCH(Rps), PING_SCHEME_CACHE, TAuditMode::NonModifying());
    SETUP_DEBUG_METHOD(PingTxProxy, TxProxyRequest, TxProxyResponse, DoTxProxyPing, RLSWITCH(Rps), PING_TX_PROXY, TAuditMode::NonModifying());
    SETUP_DEBUG_METHOD(PingActorChain, ActorChainRequest, ActorChainResponse, DoActorChainPing, RLSWITCH(Rps), PING_ACTOR_CHAIN, TAuditMode::NonModifying());

#undef SETUP_DEBUG_METHOD

}

} // namespace NKikimr::NGRpcService
