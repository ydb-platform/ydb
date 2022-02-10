#include "grpc_service.h" 
 
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
 
namespace NKikimr::NQuoter { 
 
TRateLimiterGRpcService::TRateLimiterGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
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
 
#ifdef SETUP_METHOD 
#error SETUP_METHOD macro collision 
#endif 
 
#define SETUP_METHOD(methodName, event)                                                      \ 
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                \ 
        Ydb::RateLimiter::Y_CAT(methodName, Request),                                        \ 
        Ydb::RateLimiter::Y_CAT(methodName, Response),                                       \ 
        TRateLimiterGRpcService>>                                                            \ 
    (                                                                                        \ 
        this,                                                                                \ 
        &Service_,                                                                           \ 
        CQ,                                                                                  \ 
        [this](NGrpc::IRequestContextBase* reqCtx) {                                   \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());               \ 
            ActorSystem->Send(GRpcRequestProxyId, new NGRpcService::event(reqCtx));          \ 
        },                                                                                   \ 
        &Ydb::RateLimiter::V1::RateLimiterService::AsyncService::Y_CAT(Request, methodName), \ 
        "RateLimiter/" Y_STRINGIZE(methodName),                                              \ 
        logger,                                                                              \
        getCounterBlock("rate_limiter", Y_STRINGIZE(methodName))                             \ 
    )->Run() 
 
    SETUP_METHOD(CreateResource, TEvCreateRateLimiterResource); 
    SETUP_METHOD(AlterResource, TEvAlterRateLimiterResource); 
    SETUP_METHOD(DropResource, TEvDropRateLimiterResource); 
    SETUP_METHOD(ListResources, TEvListRateLimiterResources); 
    SETUP_METHOD(DescribeResource, TEvDescribeRateLimiterResource); 
    SETUP_METHOD(AcquireResource, TEvAcquireRateLimiterResource);
 
#undef SETUP_METHOD 
} 
 
} // namespace NKikimr::NQuoter 
