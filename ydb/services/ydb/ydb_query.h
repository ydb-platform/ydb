#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

namespace NKikimr::NGRpcService {

class TGRpcYdbQueryService
    : public TGrpcServiceBase<Ydb::Query::V1::QueryService>
{
public:
    using TGrpcServiceBase<Ydb::Query::V1::QueryService>::TGrpcServiceBase;

    TGRpcYdbQueryService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const NActors::TActorId& proxyId,
        bool rlAllowed,
        size_t handlersPerCompletionQueue = 1);

    TGRpcYdbQueryService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TVector<NActors::TActorId>& proxies,
        bool rlAllowed,
        size_t handlersPerCompletionQueue);

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    const size_t HandlersPerCompletionQueue;
};

} // namespace NKikimr::NGRpcService
