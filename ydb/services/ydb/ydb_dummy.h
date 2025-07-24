#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <library/cpp/monlib/counters/counters.h>
#include <ydb/public/api/grpc/draft/dummy.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbDummyService
    : public NYdbGrpc::TGrpcServiceBase<Draft::Dummy::DummyService>
{
public:
    TGRpcYdbDummyService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId proxyActorId);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
};

}
}
