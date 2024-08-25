#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/services/deprecated/persqueue_v0/api/grpc/persqueue.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>


namespace NKikimr {

namespace NGRpcProxy {
    class TPQWriteService;
    class TPQReadService;
}

namespace NGRpcService {

class TGRpcPersQueueService
    : public NYdbGrpc::TGrpcServiceBase<NPersQueue::PersQueueService>
{
public:
    TGRpcPersQueueService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache);

    void InitService(
        const std::vector<std::unique_ptr<grpc::ServerCompletionQueue>>& cqs,
        NYdbGrpc::TLoggerPtr logger,
        size_t index) override;
    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override;
    void StopService() noexcept override;

    using NYdbGrpc::TGrpcServiceBase<NPersQueue::PersQueueService>::GetService;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem;
    std::vector<grpc::ServerCompletionQueue*> CQS;
    grpc::ServerCompletionQueue* CQ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NYdbGrpc::TGlobalLimiter* Limiter = nullptr;
    NActors::TActorId SchemeCache;

    std::shared_ptr<NGRpcProxy::TPQWriteService> WriteService;
    std::shared_ptr<NGRpcProxy::TPQReadService> ReadService;
};

} // namespace NGRpcService
} // namespace NKikimr
