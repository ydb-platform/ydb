#pragma once

#include <library/cpp/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>


namespace NKikimr {

namespace NGRpcService {
namespace V1 {

class TGRpcTopicService
    : public NGrpc::TGrpcServiceBase<Ydb::Topic::V1::TopicService>
{
public:
    TGRpcTopicService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache, const NActors::TActorId& grpcRequestProxy);

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;
    void StopService() noexcept override;

    using NGrpc::TGrpcServiceBase<Ydb::Topic::V1::TopicService>::GetService;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

    void InitNewSchemeCacheActor();

    NActors::TActorSystem* ActorSystem;
    grpc::ServerCompletionQueue* CQ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NGrpc::TGlobalLimiter* Limiter = nullptr;
    NActors::TActorId SchemeCache;
    NActors::TActorId NewSchemeCache;
    NActors::TActorId GRpcRequestProxy;
};

} // namespace V1
} // namespace NGRpcService
} // namespace NKikimr
