#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_nbs.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TNbsGRpcService::TNbsGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TNbsGRpcService::~TNbsGRpcService() = default;

void TNbsGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TNbsGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Nbs;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_NBS_METHOD
#error SETUP_NBS_METHOD macro already defined
#endif

#define SETUP_NBS_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, nbs, auditMode)

    SETUP_NBS_METHOD(CreatePartition, DoCreatePartition, RLMODE(Rps), NBS_CREATEPARTITION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Default));
    SETUP_NBS_METHOD(DeletePartition, DoDeletePartition, RLMODE(Rps), NBS_DELETEPARTITION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Default));
    SETUP_NBS_METHOD(GetLoadActorAdapterActorId, DoGetLoadActorAdapterActorId, RLMODE(Rps), NBS_GETLOADACTORADAPTERACTORID, TAuditMode::NonModifying());
    SETUP_NBS_METHOD(ListPartitions, DoListPartitions, RLMODE(Rps), NBS_LISTPARTITIONS, TAuditMode::NonModifying());

    SETUP_NBS_METHOD(WriteBlocks, DoWriteBlocks, RLMODE(Rps), NBS_WRITEBLOCKS, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Default));
    SETUP_NBS_METHOD(ReadBlocks, DoReadBlocks, RLMODE(Rps), NBS_READBLOCKS, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Default));

#undef SETUP_NBS_METHOD
}

} // namespace NKikimr::NGRpcService
