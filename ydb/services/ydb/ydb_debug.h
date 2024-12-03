#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/public/api/grpc/ydb_debug_v1.grpc.pb.h>

namespace NKikimr::NGRpcService {

class TGRpcYdbDebugService
    : public TGrpcServiceBase<Ydb::Debug::V1::DebugService>
{
public:
    using TGrpcServiceBase<Ydb::Debug::V1::DebugService>::TGrpcServiceBase;

    TGRpcYdbDebugService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const NActors::TActorId& proxyId,
        bool rlAllowed,
        size_t handlersPerCompletionQueue = 1);

    TGRpcYdbDebugService(
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
