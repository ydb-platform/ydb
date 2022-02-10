#include "grpc_service.h" 
 
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
 
namespace NKikimr { 
namespace NGRpcService { 
 
TGRpcCmsService::TGRpcCmsService(NActors::TActorSystem *system, 
                                 TIntrusivePtr<NMonitoring::TDynamicCounters> counters, 
                                 NActors::TActorId id)
    : ActorSystem_(system) 
    , Counters_(counters) 
    , GRpcRequestProxyId_(id) 
{ 
} 
 
void TGRpcCmsService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq; 
    SetupIncomingRequests(std::move(logger));
} 
 
void TGRpcCmsService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter)
{ 
    Limiter_ = limiter; 
} 
 
bool TGRpcCmsService::IncRequest() { 
    return Limiter_->Inc(); 
} 
 
void TGRpcCmsService::DecRequest() { 
    Limiter_->Dec(); 
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0); 
} 
 
void TGRpcCmsService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_); 
#ifdef ADD_REQUEST 
#error ADD_REQUEST macro already defined 
#endif 
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \ 
    MakeIntrusive<TGRpcRequest<Ydb::Cms::IN, Ydb::Cms::OUT, TGRpcCmsService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Cms::V1::CmsService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("cms", #NAME))->Run();
 
    ADD_REQUEST(CreateDatabase, CreateDatabaseRequest, CreateDatabaseResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvCreateTenantRequest(ctx)); 
    }) 
    ADD_REQUEST(AlterDatabase, AlterDatabaseRequest, AlterDatabaseResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvAlterTenantRequest(ctx)); 
    }) 
    ADD_REQUEST(GetDatabaseStatus, GetDatabaseStatusRequest, GetDatabaseStatusResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvGetTenantStatusRequest(ctx)); 
    }) 
    ADD_REQUEST(ListDatabases, ListDatabasesRequest, ListDatabasesResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvListTenantsRequest(ctx)); 
    }) 
    ADD_REQUEST(RemoveDatabase, RemoveDatabaseRequest, RemoveDatabaseResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvRemoveTenantRequest(ctx)); 
    }) 
    ADD_REQUEST(DescribeDatabaseOptions, DescribeDatabaseOptionsRequest, DescribeDatabaseOptionsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDescribeTenantOptionsRequest(ctx)); 
    }) 
 
#undef ADD_REQUEST 
} 
 
} // namespace NGRpcService 
} // namespace NKikimr 
