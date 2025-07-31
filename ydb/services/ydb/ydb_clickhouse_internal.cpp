#include "ydb_clickhouse_internal.h"

#include <ydb/core/grpc_services/service_chinternal.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

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
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    auto getLimiter = CreateLimiterCb(LimiterRegistry_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB, AUDIT_MODE) \
    MakeIntrusive<TGRpcRequest<Ydb::ClickhouseInternal::IN, Ydb::ClickhouseInternal::OUT, TGRpcYdbClickhouseInternalService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::ClickhouseInternal::IN, Ydb::ClickhouseInternal::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{RLSWITCH(NGRpcService::TRateLimiterMode::Rps), nullptr, AUDIT_MODE})); \
        }, &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("clickhouse_internal", #NAME), getLimiter("ClickhouseInternal", #NAME, DEFAULT_MAX_IN_FLIGHT))->Run();

    ADD_REQUEST(Scan, ScanRequest, ScanResponse, DoReadColumnsRequest, TAuditMode::NonModifying());
    ADD_REQUEST(GetShardLocations, GetShardLocationsRequest, GetShardLocationsResponse, DoGetShardLocationsRequest, TAuditMode::NonModifying());
    ADD_REQUEST(DescribeTable, DescribeTableRequest, DescribeTableResponse, DoKikhouseDescribeTableRequest, TAuditMode::NonModifying());
    ADD_REQUEST(CreateSnapshot, CreateSnapshotRequest, CreateSnapshotResponse, DoKikhouseCreateSnapshotRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    ADD_REQUEST(RefreshSnapshot, RefreshSnapshotRequest, RefreshSnapshotResponse, DoKikhouseRefreshSnapshotRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    ADD_REQUEST(DiscardSnapshot, DiscardSnapshotRequest, DiscardSnapshotResponse, DoKikhouseDiscardSnapshotRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
