#pragma once

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>


namespace NKikimr::NGRpcService {

class TBridgeGRpcService
        : public NYdbGrpc::TGrpcServiceBase<Ydb::Bridge::V1::BridgeService>
{
public:
    TBridgeGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId);
    ~TBridgeGRpcService();

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
