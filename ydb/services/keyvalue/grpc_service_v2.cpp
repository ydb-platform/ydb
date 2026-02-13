#include "grpc_service_v2.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_keyvalue.h>
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr::NGRpcService {

namespace {

TKeyValueRequestSettings GetKeyValueRequestSettings(NActors::TActorSystem* actorSystem) {
    TKeyValueRequestSettings settings;
    const TAppData* appData = actorSystem ? actorSystem->AppData<TAppData>() : nullptr;
    if (!appData || !appData->Icb) {
        return settings;
    }

    const auto& controls = appData->Icb->KeyValueVolumeControls;
    settings.UseCustomSerialization = controls.UseCustomSerialization.AtomicLoad()->Get();
    return settings;
}

} // namespace

TKeyValueGRpcServiceV2::TKeyValueGRpcServiceV2(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId)
    : ActorSystem_(actorSystem)
    , Counters_(std::move(counters))
    , GRpcRequestProxyId_(grpcRequestProxyId)
{
}

TKeyValueGRpcServiceV2::~TKeyValueGRpcServiceV2() = default;

void TKeyValueGRpcServiceV2::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TKeyValueGRpcServiceV2::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    Y_UNUSED(logger);
    using namespace Ydb::KeyValue;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_KV_METHOD
#error SETUP_KV_METHOD macro already defined
#endif

#define SETUP_KV_METHOD(methodName, methodCallback, rlMode, requestType, auditMode)             \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                                                                \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                                         \
        Y_CAT(methodName, Result),                                                        \
        methodCallback,                                                                                   \
        rlMode,                                                                                           \
        requestType,                                                                                      \
        YDB_API_DEFAULT_COUNTER_BLOCK(keyvalue_v2, methodName),                                           \
        auditMode,                                                                                        \
        COMMON,                                                                                           \
        ::NKikimr::NGRpcService::TGrpcRequestNoOperationCall,                                               \
        GRpcRequestProxyId_,                                                                              \
        CQ_,                                                                                              \
        nullptr,                                                                                          \
        nullptr                                                                                           \
    )

    SETUP_KV_METHOD(AcquireLock, DoAcquireLockKeyValueV2, RLMODE(Rps), KEYVALUE_ACQUIRELOCK, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ExecuteTransaction, DoExecuteTransactionKeyValueV2, RLMODE(Rps), KEYVALUE_EXECUTETRANSACTION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_KV_METHOD(Read,
            [this](std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& facility) {
                DoReadKeyValueV2(std::move(p), facility, GetKeyValueRequestSettings(ActorSystem_));
            },
            RLMODE(Rps), KEYVALUE_READ, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ReadRange,
            [this](std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& facility) {
                DoReadRangeKeyValueV2(std::move(p), facility, GetKeyValueRequestSettings(ActorSystem_));
            },
            RLMODE(Rps), KEYVALUE_READRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(ListRange, DoListRangeKeyValueV2, RLMODE(Rps), KEYVALUE_LISTRANGE, TAuditMode::NonModifying());
    SETUP_KV_METHOD(GetStorageChannelStatus, DoGetStorageChannelStatusKeyValueV2, RLMODE(Rps), KEYVALUE_GETSTORAGECHANNELSTATUS, TAuditMode::NonModifying());

#undef SETUP_KV_METHOD
}

} // namespace NKikimr::NGRpcService
