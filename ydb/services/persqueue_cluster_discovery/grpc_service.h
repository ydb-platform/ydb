#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <library/cpp/grpc/server/grpc_server.h>

namespace NKikimr::NGRpcService {

class TGRpcPQClusterDiscoveryService
   : public NGrpc::TGrpcServiceBase<Ydb::PersQueue::V1::ClusterDiscoveryService> {
public:
    TGRpcPQClusterDiscoveryService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                   NActors::TActorId id, const TMaybe<ui64>& requestsInflightLimit = Nothing());

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;
    bool IncRequest();
    void DecRequest();
    void StopService() noexcept override;

    using NGrpc::TGrpcServiceBase<Ydb::PersQueue::V1::ClusterDiscoveryService>::GetService;
private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
    THolder<NGrpc::TGlobalLimiter> Limiter;
};

}
