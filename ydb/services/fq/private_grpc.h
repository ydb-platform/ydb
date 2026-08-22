#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/core/fq/libs/grpc/fq_private_v1.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcFqPrivateTaskService
    : public NYdbGrpc::TGrpcServiceBase<Fq::Private::V1::FqPrivateTaskService>
{
public:
    TGRpcFqPrivateTaskService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
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
