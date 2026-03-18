#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_test_shard.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TTestShardSetGRpcService::TTestShardSetGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TTestShardSetGRpcService::~TTestShardSetGRpcService() = default;

void TTestShardSetGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TTestShardSetGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::TestShardSet;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_TESTSHARDSET_METHOD
#error SETUP_TESTSHARDSET_METHOD macro already defined
#endif

#define SETUP_TESTSHARDSET_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, testshardset, auditMode, EEmptyDatabaseMode::EmptyDatabaseForbidden)

    SETUP_TESTSHARDSET_METHOD(CreateTestShardSet, DoCreateTestShardSet, RLMODE(Rps), TESTSHARDSET_CREATETESTSHARDSET, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_TESTSHARDSET_METHOD(DeleteTestShardSet, DoDeleteTestShardSet, RLMODE(Rps), TESTSHARDSET_DELETETESTSHARDSET, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

#undef SETUP_TESTSHARDSET_METHOD
}

} // namespace NKikimr::NGRpcService
