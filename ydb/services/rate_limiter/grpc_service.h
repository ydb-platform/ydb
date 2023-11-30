#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_rate_limiter_v1.grpc.pb.h>

namespace NKikimr::NQuoter {

class TRateLimiterGRpcService
    : public NYdbGrpc::TGrpcServiceBase<Ydb::RateLimiter::V1::RateLimiterService>
{
public:
    TRateLimiterGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId);
    ~TRateLimiterGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem = nullptr;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    NActors::TActorId GRpcRequestProxyId;

    grpc::ServerCompletionQueue* CQ = nullptr;
    NYdbGrpc::TGlobalLimiter* Limiter = nullptr;
};

} // namespace NKikimr::NQuoter
