#include "grpc_service_v2.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_keyvalue.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

TKeyValueV2GRpcService::TKeyValueV2GRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TKeyValueV2GRpcService::~TKeyValueV2GRpcService() = default;

void TKeyValueV2GRpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TKeyValueV2GRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::KeyValue;
    Y_UNUSED(logger);
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

    #ifdef SETUP_KV_METHOD
    #error SETUP_KV_METHOD macro already defined
    #endif

    #define SETUP_RUNTIME_EVENT_METHOD_KV(methodName, method, rlMode, requestType, serviceType, counterName, auditMode, runtimeEventType)    \
        MakeIntrusive<NGRpcService::TGRpcRequest<                                                         \
            Ydb::serviceType::Y_CAT(methodName, Request),                                                 \
            Ydb::serviceType::Y_CAT(methodName, Result),                                                \
            T##serviceType##V2GRpcService>>                                                                 \
        (                                                                                                 \
            this,                                                                                         \
            &Service_,                                                                                    \
            CQ_,                                                                                           \
            [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                               \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer());                        \
                ActorSystem_->Send(GRpcRequestProxyId_, new TGrpcRequestNoOperationCall<                      \
                    Ydb::serviceType::Y_CAT(methodName, Request),                                         \
                    Ydb::serviceType::Y_CAT(methodName, Result),                                        \
                    NRuntimeEvents::EType::runtimeEventType>(reqCtx, &method,                             \
                        TRequestAuxSettings {                                                             \
                            .RlMode = TRateLimiterMode::rlMode,                                           \
                            .AuditMode = auditMode,                                                       \
                            .RequestType = NJaegerTracing::ERequestType::requestType,                     \
                        }));                                                                              \
            },                                                                                            \
            &Ydb::serviceType::V2::Y_CAT(serviceType, Service)::AsyncService::Y_CAT(Request, methodName), \
            Y_STRINGIZE(serviceType) "/" Y_STRINGIZE(methodName),                                         \
            logger,                                                                                       \
            getCounterBlock(Y_STRINGIZE(counterName), Y_STRINGIZE(methodName))                            \
        )->Run()

    #define SETUP_KV_METHOD(methodName, methodCallback, rlMode, requestType, auditMode)             \
        SETUP_RUNTIME_EVENT_METHOD_KV(methodName,                                                                             \
            methodCallback,                                                                                   \
            rlMode,                                                                                           \
            requestType,       \
            KeyValue,                                                    \
            keyvalue_v2, \
            auditMode,                                                                                        \
            COMMON                                                                                           \
        )


    SETUP_KV_METHOD(AcquireLock, DoAcquireLockKeyValueV2, Rps, KEYVALUE_ACQUIRELOCK, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ExecuteTransaction, DoExecuteTransactionKeyValueV2, Rps, KEYVALUE_EXECUTETRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_KV_METHOD(Read, DoReadKeyValueV2, Rps, KEYVALUE_READ, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ReadRange, DoReadRangeKeyValueV2, Rps, KEYVALUE_READRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ListRange, DoListRangeKeyValueV2, Rps, KEYVALUE_LISTRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(GetStorageChannelStatus, DoGetStorageChannelStatusKeyValueV2, Rps, KEYVALUE_GETSTORAGECHANNELSTATUS, TAuditMode::NonModifying());

#undef SETUP_KV_METHOD
}

} // namespace NKikimr::NGRpcService
