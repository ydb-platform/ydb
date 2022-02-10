#include "ydb_clickhouse_internal.h" 
 
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
 
namespace NKikimr { 
namespace NGRpcService { 
 
TGRpcYdbClickhouseInternalService::TGRpcYdbClickhouseInternalService(NActors::TActorSystem *system, 
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters, 
    TIntrusivePtr<TInFlightLimiterRegistry> inFlightLimiterRegistry, 
    NActors::TActorId id)
    : ActorSystem_(system) 
    , Counters_(counters) 
    , LimiterRegistry_(inFlightLimiterRegistry) 
    , GRpcRequestProxyId_(id) {} 
 
void TGRpcYdbClickhouseInternalService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq; 
    SetupIncomingRequests(std::move(logger));
} 
 
void TGRpcYdbClickhouseInternalService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter; 
} 
 
bool TGRpcYdbClickhouseInternalService::IncRequest() { 
    return Limiter_->Inc(); 
} 
 
void TGRpcYdbClickhouseInternalService::DecRequest() { 
    Limiter_->Dec(); 
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0); 
} 
 
void TGRpcYdbClickhouseInternalService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_); 
    auto getLimiter = CreateLimiterCb(LimiterRegistry_); 
 
#ifdef ADD_REQUEST 
#error ADD_REQUEST macro already defined 
#endif 
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \ 
    MakeIntrusive<TGRpcRequest<Ydb::ClickhouseInternal::IN, Ydb::ClickhouseInternal::OUT, TGRpcYdbClickhouseInternalService>>(this, &Service_, CQ_, \ 
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \ 
            ACTION; \ 
        }, &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::AsyncService::Request ## NAME, \ 
        #NAME, logger, getCounterBlock("clickhouse_internal", #NAME), getLimiter("ClickhouseInternal", #NAME, DEFAULT_MAX_IN_FLIGHT))->Run();
 
    ADD_REQUEST(Scan, ScanRequest, ScanResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvReadColumnsRequest(ctx)); 
    }) 
    ADD_REQUEST(GetShardLocations, GetShardLocationsRequest, GetShardLocationsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvGetShardLocationsRequest(ctx)); 
    }) 
    ADD_REQUEST(DescribeTable, DescribeTableRequest, DescribeTableResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvKikhouseDescribeTableRequest(ctx)); 
    }) 
    ADD_REQUEST(CreateSnapshot, CreateSnapshotRequest, CreateSnapshotResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvKikhouseCreateSnapshotRequest(ctx));
    })
    ADD_REQUEST(RefreshSnapshot, RefreshSnapshotRequest, RefreshSnapshotResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvKikhouseRefreshSnapshotRequest(ctx));
    })
    ADD_REQUEST(DiscardSnapshot, DiscardSnapshotRequest, DiscardSnapshotResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvKikhouseDiscardSnapshotRequest(ctx));
    })
#undef ADD_REQUEST 
} 
 
} // namespace NGRpcService 
} // namespace NKikimr 
