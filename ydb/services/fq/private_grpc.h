#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/core/fq/libs/grpc/fq_private_v1.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcFqPrivateTaskService
    : public NYdbGrpc::TGrpcServiceBase<Fq::Private::V1::FqPrivateTaskService>
{
public:
    TGRpcFqPrivateTaskService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        NActors::TActorId id);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
    NYdbGrpc::TGlobalLimiter* Limiter_ = nullptr;
};

} // namespace NGRpcService
} // namespace NKikimr
