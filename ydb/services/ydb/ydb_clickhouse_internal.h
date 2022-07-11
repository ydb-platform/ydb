#pragma once

#include <ydb/public/api/grpc/draft/ydb_clickhouse_internal_v1.grpc.pb.h>

#include <ydb/core/grpc_services/grpc_helper.h>
#include <library/cpp/grpc/server/grpc_server.h>

#include <library/cpp/actors/core/actorsystem.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbClickhouseInternalService
    : public NGrpc::TGrpcServiceBase<Ydb::ClickhouseInternal::V1::ClickhouseInternalService>
{
private:
    constexpr static i64 DEFAULT_MAX_IN_FLIGHT = 200;

public:
    TGRpcYdbClickhouseInternalService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> inFlightLimiterRegistry, NActors::TActorId id);

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();
private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> LimiterRegistry_;
    NActors::TActorId GRpcRequestProxyId_;
    NGrpc::TGlobalLimiter* Limiter_ = nullptr;
};

} // namespace NGRpcService
} // namespace NKikimr
