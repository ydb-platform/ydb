#pragma once

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>
#include <library/cpp/actors/core/actorsystem.h>


namespace NKikimr::NGRpcService {

class TKeyValueGRpcService
        : public NGrpc::TGrpcServiceBase<Ydb::KeyValue::V1::KeyValueService>
{
public:
    TKeyValueGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId);
    ~TKeyValueGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem = nullptr;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NActors::TActorId GRpcRequestProxyId;

    grpc::ServerCompletionQueue* CQ = nullptr;
    NGrpc::TGlobalLimiter* Limiter = nullptr;
};

} // namespace NKikimr::NGRpcService
