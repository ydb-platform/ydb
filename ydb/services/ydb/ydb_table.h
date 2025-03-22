#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/core/grpc_services/grpc_helper.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbTableService
    : public TGrpcServiceBase<Ydb::Table::V1::TableService>
{
    constexpr static i64 UNLIMITED_INFLIGHT = 0;
public:
    using TGrpcServiceBase<Ydb::Table::V1::TableService>::TGrpcServiceBase;

    TGRpcYdbTableService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        TIntrusivePtr<TInFlightLimiterRegistry> inFlightLimiterRegistry,
        const NActors::TActorId& proxyId,
        bool rlAllowed,
        size_t handlersPerCompletionQueue = 1);

    TGRpcYdbTableService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        TIntrusivePtr<TInFlightLimiterRegistry> inFlightLimiterRegistry,
        const TVector<NActors::TActorId>& proxies,
        bool rlAllowed,
        size_t handlersPerCompletionQueue);

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    const size_t HandlersPerCompletionQueue;
    TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> LimiterRegistry_;
};

} // namespace NGRpcService
} // namespace NKikimr
