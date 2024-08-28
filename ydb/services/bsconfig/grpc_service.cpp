#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_bsconfig.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>


namespace NKikimr::NGRpcService {

TBSConfigGRpcService::TBSConfigGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TBSConfigGRpcService::~TBSConfigGRpcService() = default;

void TBSConfigGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TBSConfigGRpcService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) {
    Limiter = limiter;
}

bool TBSConfigGRpcService::IncRequest() {
    return Limiter->Inc();
}

void TBSConfigGRpcService::DecRequest() {
    Limiter->Dec();
}

void TBSConfigGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

#ifdef SETUP_METHOD
#error SETUP_METHOD macro collision
#endif

#define SETUP_METHOD(methodName, method, rlMode, requestType)                                       \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                                \
        Ydb::BSConfig::Y_CAT(methodName, Request),                                                  \
        Ydb::BSConfig::Y_CAT(methodName, Response),                                                 \
        TBSConfigGRpcService>>                                                                        \
    (                                                                                                        \
        this,                                                                                                \
        &Service_,                                                                                           \
        CQ,                                                                                                  \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                                         \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                               \
            ActorSystem->Send(GRpcRequestProxyId, new TGrpcRequestOperationCall<                             \
                Ydb::BSConfig::Y_CAT(methodName, Request),                                          \
                Ydb::BSConfig::Y_CAT(methodName, Response)>(reqCtx, &method,                        \
                    TRequestAuxSettings {                                                           \
                        .RlMode = TRateLimiterMode::rlMode,                                                           \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                   \
                    }));                                                  \
        },                                                                                                   \
        &Ydb::BSConfig::V1::BSConfigService::AsyncService::Y_CAT(Request, methodName),       \
        "BSConfig/" Y_STRINGIZE(methodName),                                                          \
        logger,                                                                                              \
        getCounterBlock("BSConfig", Y_STRINGIZE(methodName))                                             \
    )->Run()

    SETUP_METHOD(Define, DoDefineBSConfig, Rps, BSCONFIG_DEFINE);
    SETUP_METHOD(Fetch, DoFetchBSConfig, Rps, BSCONFIG_FETCH);

#undef SETUP_METHOD
}

} // namespace NKikimr::NGRpcService
