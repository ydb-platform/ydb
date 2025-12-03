#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_test_shard.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TTestShardGRpcService::TTestShardGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TTestShardGRpcService::~TTestShardGRpcService() = default;

void TTestShardGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TTestShardGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::TestShard;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_TESTSHARD_METHOD
#error SETUP_TESTSHARD_METHOD macro already defined
#endif

#define SETUP_TESTSHARD_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, testshard, auditMode)

    SETUP_TESTSHARD_METHOD(CreateTestShard, DoCreateTestShard, RLMODE(Rps), TESTSHARD_CREATETESTSHARD, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_TESTSHARD_METHOD(DeleteTestShard, DoDeleteTestShard, RLMODE(Rps), TESTSHARD_DELETETESTSHARD, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

#undef SETUP_TESTSHARD_METHOD
}

} // namespace NKikimr::NGRpcService
