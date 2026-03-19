#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_actor_tracing.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TActorTracingGRpcService::TActorTracingGRpcService(
    NActors::TActorSystem* actorSystem,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
    NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TActorTracingGRpcService::~TActorTracingGRpcService() = default;

void TActorTracingGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TActorTracingGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::ActorTracing;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#define SETUP_ACTOR_TRACING_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, actor_tracing, auditMode, EEmptyDatabaseMode::EmptyDatabaseAllowed)

    SETUP_ACTOR_TRACING_METHOD(TraceStart, DoActorTracingStart, RLMODE(Rps), ACTOR_TRACING_START, TAuditMode::NonModifying());
    SETUP_ACTOR_TRACING_METHOD(TraceStop, DoActorTracingStop, RLMODE(Rps), ACTOR_TRACING_STOP, TAuditMode::NonModifying());
    SETUP_ACTOR_TRACING_METHOD(TraceFetch, DoActorTracingFetch, RLMODE(Rps), ACTOR_TRACING_FETCH, TAuditMode::NonModifying());

#undef SETUP_ACTOR_TRACING_METHOD
}

} // namespace NKikimr::NGRpcService
