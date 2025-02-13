#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_bsconfig.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include "ydb/library/grpc/server/grpc_method_setup.h"

namespace NKikimr::NGRpcService {

TConfigGRpcService::TConfigGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId) \
    : ActorSystem(actorSystem) \
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TConfigGRpcService::~TConfigGRpcService() = default;

void TConfigGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TConfigGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

    #define SETUP_BS_METHOD(methodName, method, rlMode, requestType) \
        SETUP_METHOD(methodName, method, rlMode, requestType, Config, bsconfig)

    SETUP_BS_METHOD(ReplaceConfig, DoReplaceBSConfig, Rps, BSCONFIG_REPLACESTORAGECONFIG);
    SETUP_BS_METHOD(FetchConfig, DoFetchBSConfig, Rps, BSCONFIG_FETCHSTORAGECONFIG);
    SETUP_BS_METHOD(BootstrapCluster, DoBootstrapCluster, Rps, BSCONFIG_BOOTSTRAP);

    #undef SETUP_BS_METHOD
}

} // namespace NKikimr::NGRpcService
