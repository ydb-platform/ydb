#pragma once

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem.h>


namespace NKikimr::NGRpcService {

class TKeyValueGRpcService
        : public NYdbGrpc::TGrpcServiceBase<Ydb::KeyValue::V1::KeyValueService>
{
public:
    TKeyValueGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId);
    ~TKeyValueGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem = nullptr;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NActors::TActorId GRpcRequestProxyId;

    grpc::ServerCompletionQueue* CQ = nullptr;
    NYdbGrpc::TGlobalLimiter* Limiter = nullptr;
};

} // namespace NKikimr::NGRpcService
