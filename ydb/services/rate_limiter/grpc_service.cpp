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
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TRateLimiterGRpcService::~TRateLimiterGRpcService() = default;

void TRateLimiterGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TRateLimiterGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters_, ActorSystem_);

    using namespace Ydb::RateLimiter;
    using namespace NGRpcService;

    #define SETUP_RL_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
        SETUP_METHOD(methodName, methodCallback, rlMode, requestType, rate_limiter, auditMode)

    SETUP_RL_METHOD(CreateResource, DoCreateRateLimiterResource, RLMODE(Rps), RATELIMITER_CREATE_RESOURCE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(AlterResource, DoAlterRateLimiterResource, RLMODE(Rps), RATELIMITER_ALTER_RESOURCE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(DropResource, DoDropRateLimiterResource, RLMODE(Rps), RATELIMITER_DROP_RESOURCE, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_RL_METHOD(ListResources, DoListRateLimiterResources, RLMODE(Rps), RATELIMITER_LIST_RESOURCES, TAuditMode::NonModifying());
    SETUP_RL_METHOD(DescribeResource, DoDescribeRateLimiterResource, RLMODE(Rps), RATELIMITER_DESCRIBE_RESOURCE, TAuditMode::NonModifying());
    SETUP_RL_METHOD(AcquireResource, DoAcquireRateLimiterResource, RLMODE(Off), RATELIMITER_ACQUIRE_RESOURCE, TAuditMode::NonModifying());

    #undef SETUP_RL_METHOD
}

} // namespace NKikimr::NQuoter
