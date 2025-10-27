#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_config.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include "ydb/library/grpc/server/grpc_method_setup.h"

namespace NKikimr::NGRpcService {

TConfigGRpcService::TConfigGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId) \
    : ActorSystem_(actorSystem) \
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TConfigGRpcService::~TConfigGRpcService() = default;

void TConfigGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TConfigGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters_, ActorSystem_);

    using namespace Ydb::Config;

    #define SETUP_BS_METHOD(methodName, method, rlMode, requestType, auditModeFlags) \
        SETUP_METHOD(methodName, method, rlMode, requestType, config, auditModeFlags)

    SETUP_BS_METHOD(ReplaceConfig, DoReplaceConfig, Rps, CONFIG_REPLACECONFIG, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_BS_METHOD(FetchConfig, DoFetchConfig, Rps, CONFIG_FETCHCONFIG, TAuditMode::NonModifying());

    #undef SETUP_BS_METHOD


    #define SETUP_BOOTSTRAP_CLUSTER_METHOD(methodName, method, rlMode, requestType, auditModeFlags) \
        SETUP_RUNTIME_EVENT_METHOD(methodName, YDB_API_DEFAULT_REQUEST_TYPE(methodName), YDB_API_DEFAULT_RESPONSE_TYPE(methodName), method, rlMode, requestType, config, auditModeFlags, BOOTSTRAP_CLUSTER, TGrpcRequestOperationCall, GRpcRequestProxyId_)

    SETUP_BOOTSTRAP_CLUSTER_METHOD(BootstrapCluster, DoBootstrapCluster, Rps, CONFIG_BOOTSTRAP, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

    #undef SETUP_BS_METHOD_WITH_TYPE
}

} // namespace NKikimr::NGRpcService
