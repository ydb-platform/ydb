#include "ydb_debug.h"

#include <ydb/core/grpc_services/service_debug.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>

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
                [](NYdbGrpc::IRequestContextBase* ctx) {
                    NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());
                    PlainGrpcResponse response;
                    auto ts = TInstant::Now();
                    response.SetCallBackTs(ts.MicroSeconds());
                    ctx->Reply(&response, 0);
                }, &Ydb::Debug::V1::DebugService::AsyncService::RequestPingPlainGrpc,
                "PingPlainGrpc", logger, getCounterBlock("ping", "PingPlainGrpc"))->Run();
        }
    }

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB, REQUEST_TYPE) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {  \
        for (auto* cq: CQS) { \
            MakeIntrusive<TGRpcRequest<IN, OUT, TGRpcYdbDebugService>>(this, &Service_, cq, \
                [this, proxyCounter](NYdbGrpc::IRequestContextBase* ctx) { \
                    NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
                    ActorSystem_->Send(GRpcProxies_[proxyCounter % GRpcProxies_.size()], \
                        new TGrpcRequestNoOperationCall<IN, OUT> \
                            (ctx, &CB, TRequestAuxSettings { \
                                .RlMode = RLSWITCH(TRateLimiterMode::Rps), \
                                .RequestType = NJaegerTracing::ERequestType::PING_##REQUEST_TYPE, \
                            })); \
                }, &Ydb::Debug::V1::DebugService::AsyncService::Request ## NAME, \
                #NAME, logger, getCounterBlock("query", #NAME))->Run(); \
            ++proxyCounter; \
        }  \
    }

    ADD_REQUEST(PingGrpcProxy, GrpcProxyRequest, GrpcProxyResponse, DoGrpcProxyPing, PROXY);
    ADD_REQUEST(PingKqpProxy, KqpProxyRequest, KqpProxyResponse, DoKqpPing, KQP);
    ADD_REQUEST(PingSchemeCache, SchemeCacheRequest, SchemeCacheResponse, DoSchemeCachePing, SCHEME_CACHE);
    ADD_REQUEST(PingTxProxy, TxProxyRequest, TxProxyResponse, DoTxProxyPing, TX_PROXY);
    ADD_REQUEST(PingActorChain, ActorChainRequest, ActorChainResponse, DoActorChainPing, ACTOR_CHAIN);

#undef ADD_REQUEST

}

} // namespace NKikimr::NGRpcService
