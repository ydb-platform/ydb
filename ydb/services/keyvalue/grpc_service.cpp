#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_keyvalue.h>


namespace NKikimr::NGRpcService {

TKeyValueGRpcService::TKeyValueGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem(actorSystem)
    , Counters(std::move(counters))
    , GRpcRequestProxyId(grpcRequestProxyId)
{
}

TKeyValueGRpcService::~TKeyValueGRpcService() = default;

void TKeyValueGRpcService::InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) {
    CQ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TKeyValueGRpcService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter = limiter;
}

bool TKeyValueGRpcService::IncRequest() {
    return Limiter->Inc();
}

void TKeyValueGRpcService::DecRequest() {
    Limiter->Dec();
}

void TKeyValueGRpcService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, ActorSystem);

#ifdef SETUP_METHOD
#error SETUP_METHOD macro collision
#endif

#define SETUP_METHOD(methodName, method, rlMode)                                                             \
    MakeIntrusive<NGRpcService::TGRpcRequest<                                                                \
        Ydb::KeyValue::Y_CAT(methodName, Request),                                                  \
        Ydb::KeyValue::Y_CAT(methodName, Response),                                                 \
        TKeyValueGRpcService>>                                                                        \
    (                                                                                                        \
        this,                                                                                                \
        &Service_,                                                                                           \
        CQ,                                                                                                  \
        [this](NGrpc::IRequestContextBase* reqCtx) {                                                         \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem, reqCtx->GetPeer());                               \
            ActorSystem->Send(GRpcRequestProxyId, new TGrpcRequestOperationCall<                             \
                Ydb::KeyValue::Y_CAT(methodName, Request),                                          \
                Ydb::KeyValue::Y_CAT(methodName, Response)>(reqCtx, &method,                        \
                    TRequestAuxSettings{rlMode, nullptr}));                                                  \
        },                                                                                                   \
        &Ydb::KeyValue::V1::KeyValueService::AsyncService::Y_CAT(Request, methodName),       \
        "KeyValue/" Y_STRINGIZE(methodName),                                                          \
        logger,                                                                                              \
        getCounterBlock("keyvalue", Y_STRINGIZE(methodName))                                             \
    )->Run()

    SETUP_METHOD(CreateVolume, DoCreateVolumeKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(DropVolume, DoDropVolumeKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(AlterVolume, DoAlterVolumeKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(DescribeVolume, DoDescribeVolumeKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(ListLocalPartitions, DoListLocalPartitionsKeyValue, TRateLimiterMode::Rps);

    SETUP_METHOD(AcquireLock, DoAcquireLockKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(ExecuteTransaction, DoExecuteTransactionKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(Read, DoReadKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(ReadRange, DoReadRangeKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(ListRange, DoListRangeKeyValue, TRateLimiterMode::Rps);
    SETUP_METHOD(GetStorageChannelStatus, DoGetStorageChannelStatusKeyValue, TRateLimiterMode::Rps);

#undef SETUP_METHOD
}

} // namespace NKikimr::NGRpcService
