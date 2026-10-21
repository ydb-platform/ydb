#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_distributed_storage.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TDistributedStorageGRpcService::TDistributedStorageGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TDistributedStorageGRpcService::~TDistributedStorageGRpcService() = default;

void TDistributedStorageGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TDistributedStorageGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::DistributedStorage;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_DISTRIBUTED_STORAGE_METHOD
#error SETUP_DISTRIBUTED_STORAGE_METHOD macro already defined
#endif

#define SETUP_DISTRIBUTED_STORAGE_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, distributed_storage, auditMode, EEmptyDatabaseMode::EmptyDatabaseAllowed)

    SETUP_RUNTIME_EVENT_METHOD(StreamStorageState, StorageStateRequest, StorageStateResponse, DoStreamStorageState, RLMODE(Rps),
                               DISTRIBUTED_STORAGE_STREAMSTORAGESTATE,
                               YDB_API_DEFAULT_STREAM_COUNTER_BLOCK(distributed_storage, StreamStorageState), TAuditMode::NonModifying(),
                               EEmptyDatabaseMode::EmptyDatabaseAllowed, COMMON, ::NKikimr::NGRpcService::TGrpcRequestNoOperationCall,
                               GRpcRequestProxyId_, CQ_, nullptr, nullptr);
    SETUP_DISTRIBUTED_STORAGE_METHOD(ReassignVDisk, DoReassignVDisk, RLMODE(Rps), DISTRIBUTED_STORAGE_REASSIGNVDISK, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

#undef SETUP_DISTRIBUTED_STORAGE_METHOD
}

} // namespace NKikimr::NGRpcService
