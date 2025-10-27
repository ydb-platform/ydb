#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_config.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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
    using namespace Ydb::Config;
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters_, ActorSystem_);

#define SETUP_BS_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, config, auditMode)

#define SETUP_BOOTSTRAP_CLUSTER_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                 \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),          \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),         \
        methodCallback,                                    \
        rlMode,                                            \
        requestType,                                       \
        YDB_API_DEFAULT_COUNTER_BLOCK(config, methodName), \
        auditMode,                                         \
        BOOTSTRAP_CLUSTER,                                 \
        TGrpcRequestOperationCall,                         \
        GRpcRequestProxyId_,                               \
        CQ_,                                               \
        nullptr,                                           \
        nullptr)

    SETUP_BS_METHOD(ReplaceConfig, DoReplaceConfig, RLMODE(Rps), CONFIG_REPLACECONFIG, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_BS_METHOD(FetchConfig, DoFetchConfig, RLMODE(Rps), CONFIG_FETCHCONFIG, TAuditMode::NonModifying());
    SETUP_BOOTSTRAP_CLUSTER_METHOD(BootstrapCluster, DoBootstrapCluster, RLMODE(Rps), CONFIG_BOOTSTRAP, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

#undef SETUP_BS_METHOD
#undef SETUP_BS_METHOD_WITH_TYPE
}

} // namespace NKikimr::NGRpcService
