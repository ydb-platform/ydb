#pragma once

#include <ydb/public/api/grpc/ydb_coordination_v1.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>

#include <library/cpp/actors/core/actorsystem.h>

#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NKesus {

class TKesusGRpcService
    : public NGrpc::TGrpcServiceBase<Ydb::Coordination::V1::CoordinationService>
{
    class TContextBase;
    class TSessionContext;

public:
    TKesusGRpcService(
        NActors::TActorSystem* actorSystem,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        NActors::TActorId id);
    ~TKesusGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    NActors::TActorId GRpcRequestProxyId;

    grpc::ServerCompletionQueue* CQ = nullptr;
    NGrpc::TGlobalLimiter* Limiter = nullptr;
};

}
}
