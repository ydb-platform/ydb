#include "grpc_service.h"
#include "cluster_discovery_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_method_setup.h>
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

static void DoDiscoverPQClustersRequest(std::unique_ptr<IRequestOpCtx> ctx, const IFacilityProvider&) {
    auto ev = dynamic_cast<TEvDiscoverPQClustersRequest*>(ctx.release());
    Y_ENSURE(ev);

    auto evHandle = std::make_unique<NActors::IEventHandle>(
        NPQ::NClusterDiscovery::MakeClusterDiscoveryServiceID(),
        NPQ::NClusterDiscovery::MakeClusterDiscoveryServiceID(),
        ev
    );
    evHandle->Rewrite(TRpcServices::EvDiscoverPQClusters, NPQ::NClusterDiscovery::MakeClusterDiscoveryServiceID());
    NActors::TActivationContext::Send(std::move(evHandle));
}

void TGRpcPQClusterDiscoveryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::PersQueue::ClusterDiscovery;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_PQCD_METHOD
#error SETUP_PQCD_METHOD macro already defined
#endif

#define SETUP_PQCD_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, pq_cluster_discovery, auditMode)

    SETUP_PQCD_METHOD(DiscoverClusters, DoDiscoverPQClustersRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_PQCD_METHOD
}

void TGRpcPQClusterDiscoveryService::StopService() noexcept {
    TGrpcServiceBase::StopService();
}

} // namespace NKikimr::NGRpcService
