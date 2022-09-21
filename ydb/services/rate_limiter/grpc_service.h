#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_rate_limiter_v1.grpc.pb.h>

namespace NKikimr::NQuoter {

class TRateLimiterGRpcService
    : public NGrpc::TGrpcServiceBase<Ydb::RateLimiter::V1::RateLimiterService>
{
public:
    TRateLimiterGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId grpcRequestProxyId);
    ~TRateLimiterGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem = nullptr;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    NActors::TActorId GRpcRequestProxyId;

    grpc::ServerCompletionQueue* CQ = nullptr;
    NGrpc::TGlobalLimiter* Limiter = nullptr;
};

} // namespace NKikimr::NQuoter
