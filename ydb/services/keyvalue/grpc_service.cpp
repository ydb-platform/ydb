#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_keyvalue.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TKeyValueGRpcService::TKeyValueGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TKeyValueGRpcService::~TKeyValueGRpcService() = default;

void TKeyValueGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TKeyValueGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::KeyValue;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#define SETUP_KV_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD( \
        methodName, \
        methodCallback, \
        rlMode, \
        requestType, \
        keyvalue, \
        auditMode \
    )

    SETUP_KV_METHOD(CreateVolume, DoCreateVolumeKeyValue, RLMODE(Rps), KEYVALUE_CREATEVOLUME, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KV_METHOD(DropVolume, DoDropVolumeKeyValue, RLMODE(Rps), KEYVALUE_DROPVOLUME, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KV_METHOD(AlterVolume, DoAlterVolumeKeyValue, RLMODE(Rps), KEYVALUE_ALTERVOLUME, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KV_METHOD(DescribeVolume, DoDescribeVolumeKeyValue, RLMODE(Rps), KEYVALUE_DESCRIBEVOLUME, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ListLocalPartitions, DoListLocalPartitionsKeyValue, RLMODE(Rps), KEYVALUE_LISTLOCALPARTITIONS, TAuditMode::NonModifying());

    SETUP_KV_METHOD(AcquireLock, DoAcquireLockKeyValue, RLMODE(Rps), KEYVALUE_ACQUIRELOCK, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ExecuteTransaction, DoExecuteTransactionKeyValue, RLMODE(Rps), KEYVALUE_EXECUTETRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_KV_METHOD(Read, DoReadKeyValue, RLMODE(Rps), KEYVALUE_READ, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ReadRange, DoReadRangeKeyValue, RLMODE(Rps), KEYVALUE_READRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ListRange, DoListRangeKeyValue, RLMODE(Rps), KEYVALUE_LISTRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(GetStorageChannelStatus, DoGetStorageChannelStatusKeyValue, RLMODE(Rps), KEYVALUE_GETSTORAGECHANNELSTATUS, TAuditMode::NonModifying());

#undef SETUP_KV_METHOD
}

} // namespace NKikimr::NGRpcService
