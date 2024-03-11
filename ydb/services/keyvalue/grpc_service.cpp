#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_keyvalue.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>


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

void TKeyValueGRpcService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) {
    Limiter = limiter;
}

bool TKeyValueGRpcService::IncRequest() {
    return Limiter->Inc();
}

void TKeyValueGRpcService::DecRequest() {
    Limiter->Dec();
}

void TKeyValueGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

#ifdef SETUP_METHOD
#error SETUP_METHOD macro collision
#endif

#define SETUP_METHOD(methodName, method, rlMode, requestType)                                       \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                                \
        Ydb::KeyValue::Y_CAT(methodName, Request),                                                  \
        Ydb::KeyValue::Y_CAT(methodName, Response),                                                 \
        TKeyValueGRpcService>>                                                                        \
    (                                                                                                        \
        this,                                                                                                \
        &Service_,                                                                                           \
        CQ,                                                                                                  \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                                         \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                               \
            ActorSystem->Send(GRpcRequestProxyId, new TGrpcRequestOperationCall<                             \
                Ydb::KeyValue::Y_CAT(methodName, Request),                                          \
                Ydb::KeyValue::Y_CAT(methodName, Response)>(reqCtx, &method,                        \
                    TRequestAuxSettings {                                                           \
                        .RlMode = rlMode,                                                           \
                        .RequestType = NJaegerTracing::ERequestType::requestType,                   \
                    }));                                                  \
        },                                                                                                   \
        &Ydb::KeyValue::V1::KeyValueService::AsyncService::Y_CAT(Request, methodName),       \
        "KeyValue/" Y_STRINGIZE(methodName),                                                          \
        logger,                                                                                              \
        getCounterBlock("keyvalue", Y_STRINGIZE(methodName))                                             \
    )->Run()

    SETUP_METHOD(CreateVolume, DoCreateVolumeKeyValue, TRateLimiterMode::Rps, UNSPECIFIED);
    SETUP_METHOD(DropVolume, DoDropVolumeKeyValue, TRateLimiterMode::Rps, UNSPECIFIED);
    SETUP_METHOD(AlterVolume, DoAlterVolumeKeyValue, TRateLimiterMode::Rps, UNSPECIFIED);
    SETUP_METHOD(DescribeVolume, DoDescribeVolumeKeyValue, TRateLimiterMode::Rps, UNSPECIFIED);
    SETUP_METHOD(ListLocalPartitions, DoListLocalPartitionsKeyValue, TRateLimiterMode::Rps, UNSPECIFIED);

    SETUP_METHOD(AcquireLock, DoAcquireLockKeyValue, TRateLimiterMode::Rps, KEYVALUE_ACQUIRELOCK);
    SETUP_METHOD(ExecuteTransaction, DoExecuteTransactionKeyValue, TRateLimiterMode::Rps, KEYVALUE_EXECUTETRANSACTION);
    SETUP_METHOD(Read, DoReadKeyValue, TRateLimiterMode::Rps, KEYVALUE_READ);
    SETUP_METHOD(ReadRange, DoReadRangeKeyValue, TRateLimiterMode::Rps, KEYVALUE_READRANGE);
    SETUP_METHOD(ListRange, DoListRangeKeyValue, TRateLimiterMode::Rps, KEYVALUE_LISTRANGE);
    SETUP_METHOD(GetStorageChannelStatus, DoGetStorageChannelStatusKeyValue, TRateLimiterMode::Rps, KEYVALUE_GETSTORAGECHANNELSTATUS);

#undef SETUP_METHOD
}

} // namespace NKikimr::NGRpcService
