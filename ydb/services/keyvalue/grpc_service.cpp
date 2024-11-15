#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_keyvalue.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include "ydb/library/grpc/server/grpc_method_setup.h"

namespace NKikimr::NGRpcService {

TKeyValueGRpcService::TKeyValueGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TKeyValueGRpcService::~TKeyValueGRpcService() = default;

void TKeyValueGRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TKeyValueGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

    #define SETUP_KV_METHOD(methodName, method, rlMode, requestType) \
        SETUP_METHOD( \
            methodName, \
            method, \
            rlMode, \
            requestType, \
            KeyValue, \
            keyvalue \
        )

    SETUP_KV_METHOD(CreateVolume, DoCreateVolumeKeyValue, Rps, KEYVALUE_CREATEVOLUME);
    SETUP_KV_METHOD(DropVolume, DoDropVolumeKeyValue, Rps, KEYVALUE_DROPVOLUME);
    SETUP_KV_METHOD(AlterVolume, DoAlterVolumeKeyValue, Rps, KEYVALUE_ALTERVOLUME);
    SETUP_KV_METHOD(DescribeVolume, DoDescribeVolumeKeyValue, Rps, KEYVALUE_DESCRIBEVOLUME);
    SETUP_KV_METHOD(ListLocalPartitions, DoListLocalPartitionsKeyValue, Rps, KEYVALUE_LISTLOCALPARTITIONS);

    SETUP_KV_METHOD(AcquireLock, DoAcquireLockKeyValue, Rps, KEYVALUE_ACQUIRELOCK);
    SETUP_KV_METHOD(ExecuteTransaction, DoExecuteTransactionKeyValue, Rps, KEYVALUE_EXECUTETRANSACTION);
    SETUP_KV_METHOD(Read, DoReadKeyValue, Rps, KEYVALUE_READ);
    SETUP_KV_METHOD(ReadRange, DoReadRangeKeyValue, Rps, KEYVALUE_READRANGE);
    SETUP_KV_METHOD(ListRange, DoListRangeKeyValue, Rps, KEYVALUE_LISTRANGE);
    SETUP_KV_METHOD(GetStorageChannelStatus, DoGetStorageChannelStatusKeyValue, Rps, KEYVALUE_GETSTORAGECHANNELSTATUS);

    #undef SETUP_KV_METHOD
}

} // namespace NKikimr::NGRpcService
