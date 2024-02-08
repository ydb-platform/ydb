#pragma once

#include <ydb/core/http_proxy/events.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/core/protos/serverless_proxy_config.pb.h>


namespace NKikimr::NHttpProxy {

class TGRpcDiscoveryService
    : public NYdbGrpc::TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService>
{
public:
    TGRpcDiscoveryService(NActors::TActorSystem* system, std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
                 TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_;

    std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider_;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NYdbGrpc::TGlobalLimiter* Limiter_;
};

} // namespace NKikimr
