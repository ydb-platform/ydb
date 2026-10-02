#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/actorid.h>

#include <ydb/library/grpc/server/grpc_server.h>

#include <ydb/public/api/grpc/draft/fq_v1.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcFederatedQueryService
    : public NYdbGrpc::TGrpcServiceBase<FederatedQuery::V1::FederatedQueryService>
{
public:
    TGRpcFederatedQueryService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        NActors::TActorId id);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
};

} // namespace NGRpcService
} // namespace NKikimr
