#include "private_grpc.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_fq_internal.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcFqPrivateTaskService::TGRpcFqPrivateTaskService(NActors::TActorSystem *system,
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcFqPrivateTaskService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcFqPrivateTaskService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Fq::Private;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_FQ_METHOD
#error SETUP_FQ_METHOD macro already defined
#endif

#define SETUP_FQ_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, fq_internal, auditMode)

    SETUP_FQ_METHOD(PingTask, DoFqPrivatePingTaskRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_FQ_METHOD(GetTask, DoFqPrivateGetTaskRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_FQ_METHOD(WriteTaskResult, DoFqPrivateWriteTaskResultRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_FQ_METHOD(NodesHealthCheck, DoFqPrivateNodesHealthCheckRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_FQ_METHOD(CreateRateLimiterResource, DoFqPrivateCreateRateLimiterResourceRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_FQ_METHOD(DeleteRateLimiterResource, DoFqPrivateDeleteRateLimiterResourceRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));

#undef SETUP_FQ_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
