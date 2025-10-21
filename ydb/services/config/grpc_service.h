#pragma once

#include <ydb/public/api/grpc/ydb_config_v1.grpc.pb.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/actorid.h>


namespace NKikimr::NGRpcService {

class TConfigGRpcService
        : public NYdbGrpc::TGrpcServiceBase<Ydb::Config::V1::ConfigService>
{
public:
    TConfigGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId);
    ~TConfigGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem = nullptr;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    NActors::TActorId GRpcRequestProxyId;

    grpc::ServerCompletionQueue* CQ = nullptr;
};

} // namespace NKikimr::NGRpcService
