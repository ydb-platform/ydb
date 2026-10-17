#include "grpc_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_coordination.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_streaming/grpc_streaming.h>

#include <ydb/library/grpc/server/event_callback.h>
#include <ydb/library/grpc/server/grpc_async_ctx_base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NKesus {

// Note: this is an extremely high default to avoid breaking clients
// TODO: make it configurable
static constexpr i64 DEFAULT_MAX_SESSIONS_INFLIGHT = 100000;

////////////////////////////////////////////////////////////////////////////////

TKesusGRpcService::TKesusGRpcService(
        NActors::TActorSystem* system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> limiterRegistry,
        const NActors::TActorId& proxyId,
        bool rlAllowed)
    : TBase(system, counters, proxyId, rlAllowed)
    , LimiterRegistry_(limiterRegistry)
{}

void TKesusGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using NGRpcService::TRateLimiterMode;
    using NGRpcService::TAuditMode;
    using namespace Ydb::Coordination;
    using namespace NGRpcService;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    auto getLimiter = CreateLimiterCb(LimiterRegistry_);
    auto& icb = *ActorSystem_->AppData<TAppData>()->Icb;

#ifdef SETUP_KESUS_METHOD
#error SETUP_KESUS_METHOD macro already defined
#endif

#ifdef GET_LIMITER_BY_PATH
#error GET_LIMITER_BY_PATH macro already defined
#endif

#ifdef SETUP_KESUS_STREAM_METHOD
#error SETUP_KESUS_STREAM_METHOD macro already defined
#endif

#define SETUP_KESUS_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    for (auto* cq : CQS) {                                                             \
        SETUP_RUNTIME_EVENT_METHOD(methodName,                                         \
            YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                  \
            YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                                 \
            methodCallback,                                                            \
            rlMode,                                                                    \
            requestType,                                                               \
            YDB_API_DEFAULT_COUNTER_BLOCK(coordination, methodName),                   \
            auditMode,                                                                 \
            EEmptyDatabaseMode::EmptyDatabaseForbidden,                                \
            COMMON,                                                                    \
            ::NKikimr::NGRpcService::TGrpcRequestOperationCall,                        \
            GRpcRequestProxyId_,                                                       \
            cq,                                                                        \
            nullptr,                                                                   \
            nullptr);                                                                  \
    }

#define GET_LIMITER_BY_PATH(ICB_PATH) \
    getLimiter(#ICB_PATH, icb.ICB_PATH, DEFAULT_MAX_SESSIONS_INFLIGHT)

#define SETUP_KESUS_STREAM_METHOD(methodName, rlMode, requestType, auditMode, operationCallClass)          \
    for (auto* cq : CQS) {                                                                                 \
        SETUP_RUNTIME_EVENT_STREAM_METHOD(methodName,                                                      \
            YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                                      \
            YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                                                     \
            rlMode,                                                                                        \
            requestType,                                                                                   \
            YDB_API_DEFAULT_STREAM_COUNTER_BLOCK(coordination, methodName),                                \
            auditMode,                                                                                     \
            EEmptyDatabaseMode::EmptyDatabaseForbidden,                                                    \
            operationCallClass,                                                                            \
            GRpcRequestProxyId_,                                                                           \
            cq,                                                                                            \
            GET_LIMITER_BY_PATH(GRpcControls.RequestConfigs.CoordinationService_##methodName.MaxInFlight), \
            nullptr);                                                                                      \
    }

    SETUP_KESUS_METHOD(CreateNode, DoCreateCoordinationNode, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KESUS_METHOD(AlterNode, DoAlterCoordinationNode, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KESUS_METHOD(DropNode, DoDropCoordinationNode, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_KESUS_METHOD(DescribeNode, DoDescribeCoordinationNode, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_KESUS_STREAM_METHOD(Session, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), NGRpcService::TEvCoordinationSessionRequest);

#undef GET_LIMITER_BY_PATH
#undef SETUP_KESUS_METHOD
#undef SETUP_KESUS_STREAM_METHOD
}

} // namespace NKesus
} // namespace NKikimr
