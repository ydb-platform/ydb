#pragma once

#include <library/cpp/actors/core/actorsystem.h>

#include <kikimr/yndx/api/grpc/persqueue.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>


namespace NKikimr {

namespace NGRpcProxy {
    class TPQWriteService;
    class TPQReadService;
}

namespace NGRpcService {

class TGRpcPersQueueService
    : public NGrpc::TGrpcServiceBase<NPersQueue::PersQueueService>
{
public:
    TGRpcPersQueueService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache);

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;
    void StopService() noexcept override;

    using NGrpc::TGrpcServiceBase<NPersQueue::PersQueueService>::GetService;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests();

    NActors::TActorSystem* ActorSystem;
    grpc::ServerCompletionQueue* CQ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NGrpc::TGlobalLimiter* Limiter = nullptr;
    NActors::TActorId SchemeCache;

    std::shared_ptr<NGRpcProxy::TPQWriteService> WriteService;
    std::shared_ptr<NGRpcProxy::TPQReadService> ReadService;
};

} // namespace NGRpcService
} // namespace NKikimr
