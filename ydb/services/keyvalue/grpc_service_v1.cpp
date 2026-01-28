#include "grpc_service_v1.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_keyvalue.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TKeyValueV1GRpcService::TKeyValueV1GRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TKeyValueV1GRpcService::~TKeyValueV1GRpcService() = default;

void TKeyValueV1GRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TKeyValueV1GRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::KeyValue;
    Y_UNUSED(logger);
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

    #ifdef SETUP_KV_METHOD
    #error SETUP_KV_METHOD macro already defined
    #endif


    #define SETUP_RUNTIME_EVENT_METHOD_KV(methodName, method, rlMode, requestType, serviceType, counterName, auditMode, runtimeEventType)    \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                         \
        Ydb::serviceType::Y_CAT(methodName, Request),                                                 \
        Ydb::serviceType::Y_CAT(methodName, Response),                                                \
        TKeyValueV1GRpcService>>                                                                 \
    (                                                                                                 \
        this,                                                                                         \
        &Service_,                                                                                    \
        CQ,                                                                                           \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                               \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                        \
            ActorSystem->Send(GRpcRequestProxyId, new TGrpcRequestOperationCall<                      \
                Ydb::serviceType::Y_CAT(methodName, Request),                                         \
                Ydb::serviceType::Y_CAT(methodName, Response),                                        \
                NRuntimeEvents::EType::runtimeEventType>(reqCtx, &method,                             \
                    TRequestAuxSettings {                                                             \
                        .RlMode = TRateLimiterMode::rlMode,                                           \
                        .AuditMode = auditMode,                                                       \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                     \
                    }));                                                                              \
        },                                                                                            \
        &Ydb::serviceType::V1::Y_CAT(serviceType, Service)::AsyncService::Y_CAT(Request, methodName), \
        Y_STRINGIZE(serviceType) "/" Y_STRINGIZE(methodName),                                         \
        logger,                                                                                       \
        getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                            \
    )->Run()

    #define SETUP_KV_METHOD(methodName, methodCallback, rlMode, requestType, auditMode)                       \
        SETUP_RUNTIME_EVENT_METHOD_KV(methodName,                                                             \
            methodCallback,                                                                                   \
            rlMode,                                                                                           \
            requestType,                                                                                      \
            KeyValue,                                                                                         \
            keyvalue_v1, \
            auditMode,                                                                                        \
            COMMON                                                                                            \
        )


    SETUP_KV_METHOD(CreateVolume, DoCreateVolumeKeyValue, Rps, KEYVALUE_CREATEVOLUME, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KV_METHOD(DropVolume, DoDropVolumeKeyValue, Rps, KEYVALUE_DROPVOLUME, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KV_METHOD(AlterVolume, DoAlterVolumeKeyValue, Rps, KEYVALUE_ALTERVOLUME, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KV_METHOD(DescribeVolume, DoDescribeVolumeKeyValue, Rps, KEYVALUE_DESCRIBEVOLUME, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ListLocalPartitions, DoListLocalPartitionsKeyValue, Rps, KEYVALUE_LISTLOCALPARTITIONS, TAuditMode::NonModifying());

    SETUP_KV_METHOD(AcquireLock, DoAcquireLockKeyValue, Rps, KEYVALUE_ACQUIRELOCK, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ExecuteTransaction, DoExecuteTransactionKeyValue, Rps, KEYVALUE_EXECUTETRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_KV_METHOD(Read, DoReadKeyValue, Rps, KEYVALUE_READ, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ReadRange, DoReadRangeKeyValue, Rps, KEYVALUE_READRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ListRange, DoListRangeKeyValue, Rps, KEYVALUE_LISTRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(GetStorageChannelStatus, DoGetStorageChannelStatusKeyValue, Rps, KEYVALUE_GETSTORAGECHANNELSTATUS, TAuditMode::NonModifying());

    #undef SETUP_KV_METHOD
}

} // namespace NKikimr::NGRpcService
