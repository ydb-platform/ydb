#include "ydb_clickhouse_internal.h"

#include <ydb/core/grpc_services/service_chinternal.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYdbClickhouseInternalService::TGRpcYdbClickhouseInternalService(NActors::TActorSystem *system,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    TIntrusivePtr<TInFlightLimiterRegistry> inFlightLimiterRegistry,
    NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , LimiterRegistry_(inFlightLimiterRegistry)
    , GRpcRequestProxyId_(id) {}

void TGRpcYdbClickhouseInternalService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYdbClickhouseInternalService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcYdbClickhouseInternalService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYdbClickhouseInternalService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcYdbClickhouseInternalService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    auto getLimiter = CreateLimiterCb(LimiterRegistry_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::ClickhouseInternal::IN, Ydb::ClickhouseInternal::OUT, TGRpcYdbClickhouseInternalService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::ClickhouseInternal::IN, Ydb::ClickhouseInternal::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Rps, nullptr})); \
        }, &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("clickhouse_internal", #NAME), getLimiter("ClickhouseInternal", #NAME, DEFAULT_MAX_IN_FLIGHT))->Run();

    ADD_REQUEST(Scan, ScanRequest, ScanResponse, DoReadColumnsRequest);
    ADD_REQUEST(GetShardLocations, GetShardLocationsRequest, GetShardLocationsResponse, DoGetShardLocationsRequest);
    ADD_REQUEST(DescribeTable, DescribeTableRequest, DescribeTableResponse, DoKikhouseDescribeTableRequest);
    ADD_REQUEST(CreateSnapshot, CreateSnapshotRequest, CreateSnapshotResponse, DoKikhouseCreateSnapshotRequest);
    ADD_REQUEST(RefreshSnapshot, RefreshSnapshotRequest, RefreshSnapshotResponse, DoKikhouseRefreshSnapshotRequest);
    ADD_REQUEST(DiscardSnapshot, DiscardSnapshotRequest, DiscardSnapshotResponse, DoKikhouseDiscardSnapshotRequest);
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
