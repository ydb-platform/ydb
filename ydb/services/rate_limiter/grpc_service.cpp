#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_ratelimiter.h>

namespace NKikimr::NQuoter {

TRateLimiterGRpcService::TRateLimiterGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TRateLimiterGRpcService::~TRateLimiterGRpcService() = default;

void TRateLimiterGRpcService::InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TRateLimiterGRpcService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter = limiter;
}

bool TRateLimiterGRpcService::IncRequest() {
    return Limiter->Inc();
}

void TRateLimiterGRpcService::DecRequest() {
    Limiter->Dec();
}

void TRateLimiterGRpcService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);
    using namespace NGRpcService;

#ifdef SETUP_METHOD
#error SETUP_METHOD macro collision
#endif

#define SETUP_METHOD(methodName, cb, rps)                                                    \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                \
        Ydb::RateLimiter::Y_CAT(methodName, Request),                                        \
        Ydb::RateLimiter::Y_CAT(methodName, Response),                                       \
        TRateLimiterGRpcService>>                                                            \
    (                                                                                        \
        this,                                                                                \
        &Service_,                                                                           \
        CQ,                                                                                  \
        [this](NGrpc::IRequestContextBase* reqCtx) {                                         \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());               \
            ActorSystem->Send(GRpcRequestProxyId,                                            \
                new NGRpcService::TGrpcRequestOperationCall<                                 \
                    Ydb::RateLimiter::Y_CAT(methodName, Request),                            \
                    Ydb::RateLimiter::Y_CAT(methodName, Response)>                           \
                        (reqCtx, &cb, TRequestAuxSettings{TRateLimiterMode::rps, nullptr})); \
        },                                                                                   \
        &Ydb::RateLimiter::V1::RateLimiterService::AsyncService::Y_CAT(Request, methodName), \
        "RateLimiter/" Y_STRINGIZE(methodName),                                              \
        logger,                                                                              \
        getCounterBlock("rate_limiter", Y_STRINGIZE(methodName))                             \
    )->Run()

    SETUP_METHOD(CreateResource, DoCreateRateLimiterResource, Rps);
    SETUP_METHOD(AlterResource, DoAlterRateLimiterResource, Rps);
    SETUP_METHOD(DropResource, DoDropRateLimiterResource, Rps);
    SETUP_METHOD(ListResources, DoListRateLimiterResources, Rps);
    SETUP_METHOD(DescribeResource, DoDescribeRateLimiterResource, Rps);
    SETUP_METHOD(AcquireResource, DoAcquireRateLimiterResource, Off);

#undef SETUP_METHOD
}

} // namespace NKikimr::NQuoter
