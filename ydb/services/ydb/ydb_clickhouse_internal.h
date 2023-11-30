#pragma once

#include <ydb/public/api/grpc/draft/ydb_clickhouse_internal_v1.grpc.pb.h>

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/library/grpc/server/grpc_server.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/grpc_services/base/base_service.h>


namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbClickhouseInternalService
    : public TGrpcServiceBase<Ydb::ClickhouseInternal::V1::ClickhouseInternalService>
{
private:
    constexpr static i64 DEFAULT_MAX_IN_FLIGHT = 200;

public:
    TGRpcYdbClickhouseInternalService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> inFlightLimiterRegistry, NActors::TActorId id, bool rlAllowed);

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> LimiterRegistry_;
};

} // namespace NGRpcService
} // namespace NKikimr
