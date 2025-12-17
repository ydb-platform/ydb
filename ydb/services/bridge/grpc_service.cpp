#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_bridge.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TBridgeGRpcService::TBridgeGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TBridgeGRpcService::~TBridgeGRpcService() = default;

void TBridgeGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TBridgeGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Bridge;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_BRIDGE_METHOD
#error SETUP_BRIDGE_METHOD macro already defined
#endif

#define SETUP_BRIDGE_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, bridge, auditMode)

    SETUP_BRIDGE_METHOD(GetClusterState, DoGetClusterState, RLMODE(Rps), BRIDGE_GETCLUSTERSTATE, TAuditMode::NonModifying());
    SETUP_BRIDGE_METHOD(UpdateClusterState, DoUpdateClusterState, RLMODE(Rps), BRIDGE_UPDATECLUSTERSTATE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

#undef SETUP_BRIDGE_METHOD
}

} // namespace NKikimr::NGRpcService
