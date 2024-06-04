#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/core/grpc_services/base/base_service.h>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

class TGRpcLocalDiscoveryService
    : public NYdbGrpc::TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService>
{
public:
    TGRpcLocalDiscoveryService(const NKikimrConfig::TGRpcConfig& grpcConfig,
                    NActors::TActorSystem* system,
                    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                    NActors::TActorId id);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);
    void DoListEndpointsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& provider);

    const NKikimrConfig::TGRpcConfig& GrpcConfig;
    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
    NYdbGrpc::TGlobalLimiter* Limiter_ = nullptr;
};

} // namespace NGRpcService
} // namespace NKikimr
