#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_bridge.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include "ydb/library/grpc/server/grpc_method_setup.h"

namespace NKikimr::NGRpcService {

TBridgeGRpcService::TBridgeGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TBridgeGRpcService::~TBridgeGRpcService() = default;

void TBridgeGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TBridgeGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

    #define SETUP_BRIDGE_METHOD(methodName, method, rlMode, requestType, auditModeFlags) \
        SETUP_METHOD(methodName, method, rlMode, requestType, Bridge, config, auditModeFlags)

    SETUP_BRIDGE_METHOD(GetClusterState, DoGetClusterState, Rps, BRIDGE_GETCLUSTERSTATE, TAuditMode::NonModifying());
    SETUP_BRIDGE_METHOD(UpdateClusterState, DoUpdateClusterState, Rps, BRIDGE_UPDATECLUSTERSTATE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

    #undef SETUP_BRIDGE_METHOD
}

} // namespace NKikimr::NGRpcService
