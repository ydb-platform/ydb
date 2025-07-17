#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_ratelimiter.h>
#include "ydb/library/grpc/server/grpc_method_setup.h"

namespace NKikimr::NQuoter {

TRateLimiterGRpcService::TRateLimiterGRpcService(
    NActors::TActorSystem* actorSystem,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TRateLimiterGRpcService::~TRateLimiterGRpcService() = default;

void TRateLimiterGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TRateLimiterGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);
    using namespace NGRpcService;

    #define SETUP_RL_METHOD(methodName, method, rlMode, requestType, auditModeFlags) \
        SETUP_METHOD(methodName, method, rlMode, requestType, RateLimiter, rate_limiter, auditModeFlags)

    SETUP_RL_METHOD(CreateResource, DoCreateRateLimiterResource, Rps, RATELIMITER_CREATE_RESOURCE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(AlterResource, DoAlterRateLimiterResource, Rps, RATELIMITER_ALTER_RESOURCE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(DropResource, DoDropRateLimiterResource, Rps, RATELIMITER_DROP_RESOURCE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(ListResources, DoListRateLimiterResources, Rps, RATELIMITER_LIST_RESOURCES, TAuditMode::NonModifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(DescribeResource, DoDescribeRateLimiterResource, Rps, RATELIMITER_DESCRIBE_RESOURCE, TAuditMode::NonModifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(AcquireResource, DoAcquireRateLimiterResource, Off, RATELIMITER_ACQUIRE_RESOURCE, TAuditMode::NonModifying(TAuditMode::TLogClassConfig::Dml));

    #undef SETUP_RL_METHOD
}

} // namespace NKikimr::NQuoter
