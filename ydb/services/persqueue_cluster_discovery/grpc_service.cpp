#include "grpc_service.h"
#include "cluster_discovery_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_request.h>

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NGRpcService {

TGRpcPQClusterDiscoveryService::TGRpcPQClusterDiscoveryService(
        NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId id,
        const TMaybe<ui64>& requestsInflightLimit
)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id)
{
    if (requestsInflightLimit.Defined()) {
        Limiter = MakeHolder<NYdbGrpc::TGlobalLimiter>(requestsInflightLimit.GetRef());
    }
}

void TGRpcPQClusterDiscoveryService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;

    if (ActorSystem_->AppData<TAppData>()->PQClusterDiscoveryConfig.GetEnabled()) {
        IActor* actor = NPQ::NClusterDiscovery::CreateClusterDiscoveryService(GetServiceCounters(Counters_, "persqueue")->GetSubgroup("subsystem", "cluster_discovery"));
        TActorId clusterDiscoveryServiceId = ActorSystem_->Register(actor, TMailboxType::HTSwap, ActorSystem_->AppData<TAppData>()->UserPoolId);
        ActorSystem_->RegisterLocalService(NPQ::NClusterDiscovery::MakeClusterDiscoveryServiceID(), clusterDiscoveryServiceId);

        SetupIncomingRequests(std::move(logger));
    }
}

void TGRpcPQClusterDiscoveryService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter*) {
}

bool TGRpcPQClusterDiscoveryService::IncRequest() {
    if (Limiter) {
        return Limiter->Inc();
    }
    return true;
}

void TGRpcPQClusterDiscoveryService::DecRequest() {
    if (Limiter) {
        Limiter->Dec();
    }
}

void TGRpcPQClusterDiscoveryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::PersQueue::ClusterDiscovery::IN, Ydb::PersQueue::ClusterDiscovery::OUT, TGRpcPQClusterDiscoveryService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase* ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::PersQueue::V1::ClusterDiscoveryService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("pq_cluster_discovery", #NAME))->Run();

        ADD_REQUEST(DiscoverClusters, DiscoverClustersRequest, DiscoverClustersResponse, {
            ActorSystem_->Send(GRpcRequestProxyId_, new TEvDiscoverPQClustersRequest(ctx));
        })
#undef ADD_REQUEST

}

void TGRpcPQClusterDiscoveryService::StopService() noexcept {
    TGrpcServiceBase::StopService();
}

void TGRpcRequestProxyHandleMethods::Handle(TEvDiscoverPQClustersRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Send(ev->Forward(NPQ::NClusterDiscovery::MakeClusterDiscoveryServiceID()));
}

} // namespace NKikimr::NGRpcService
