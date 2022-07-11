#pragma once

#include <ydb/core/http_proxy/events.h>

#include <library/cpp/actors/core/actorsystem.h>

#include <library/cpp/grpc/server/grpc_server.h>
#include <library/cpp/actors/core/actor.h>

#include <ydb/core/protos/serverless_proxy_config.pb.h>


namespace NKikimr::NHttpProxy {

class TGRpcDiscoveryService
    : public NGrpc::TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService>
{
public:
    TGRpcDiscoveryService(NActors::TActorSystem* system, std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
                 TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_;

    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider_;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NGrpc::TGlobalLimiter* Limiter_;
};

} // namespace NKikimr
