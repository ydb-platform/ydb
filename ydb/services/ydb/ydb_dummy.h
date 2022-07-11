#pragma once

#include <library/cpp/grpc/server/grpc_server.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/monlib/counters/counters.h>
#include <ydb/public/api/grpc/draft/dummy.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbDummyService
    : public NGrpc::TGrpcServiceBase<Draft::Dummy::DummyService>
{
public:
    TGRpcYdbDummyService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId proxyActorId);

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
    NGrpc::TGlobalLimiter* Limiter_ = nullptr;
};

}
}
