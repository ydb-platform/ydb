#include "ydb_clickhouse_internal.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/service_chinternal.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYdbClickhouseInternalService::TGRpcYdbClickhouseInternalService(NActors::TActorSystem *system,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    TIntrusivePtr<TInFlightLimiterRegistry> inFlightLimiterRegistry,
    NActors::TActorId id,
    bool rlAllowed)
    : TGrpcServiceBase<Ydb::ClickhouseInternal::V1::ClickhouseInternalService>(system, counters, id, rlAllowed)
    , LimiterRegistry_(inFlightLimiterRegistry)
{}

void TGRpcYdbClickhouseInternalService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::ClickhouseInternal;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    auto getLimiter = CreateLimiterCb(LimiterRegistry_);
    auto& icb = *ActorSystem_->AppData<TAppData>()->Icb;

#ifdef SETUP_CH_METHOD
#error SETUP_CH_METHOD macro already defined
#endif

#ifdef GET_LIMITER_BY_PATH
#error GET_LIMITER_BY_PATH macro already defined
#endif

#define GET_LIMITER_BY_PATH(ICB_PATH) \
    getLimiter(#ICB_PATH, icb.ICB_PATH, DEFAULT_MAX_IN_FLIGHT)

#define SETUP_CH_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                                          \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),                                   \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                                  \
        methodCallback,                                                             \
        rlMode,                                                                     \
        requestType,                                                                \
        YDB_API_DEFAULT_COUNTER_BLOCK(clickhouse_internal, methodName),             \
        auditMode,                                                                  \
        COMMON,                                                                     \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall,                         \
        GRpcRequestProxyId_,                                                        \
        CQ_,                                                                        \
        GET_LIMITER_BY_PATH(GRpcControls.RequestConfigs.ClickhouseInternal_##methodName.MaxInFlight), \
        nullptr)

    SETUP_CH_METHOD(Scan, DoReadColumnsRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CH_METHOD(GetShardLocations, DoGetShardLocationsRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CH_METHOD(DescribeTable, DoKikhouseDescribeTableRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_CH_METHOD(CreateSnapshot, DoKikhouseCreateSnapshotRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_CH_METHOD(RefreshSnapshot, DoKikhouseRefreshSnapshotRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_CH_METHOD(DiscardSnapshot, DoKikhouseDiscardSnapshotRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));

#undef GET_LIMITER_BY_PATH
#undef SETUP_CH_METHOD
}

} // namespace NGRpcService
} // namespace NKikimr
