#pragma once

#include <ydb/public/api/grpc/etcd/rpc.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NGRpcService {

class TEtcdGRpcService
    : public NYdbGrpc::TGrpcServiceBase<etcdserverpb::KV>
{
public:
    TEtcdGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId = {});
    ~TEtcdGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem *const ActorSystem;
    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    grpc::ServerCompletionQueue* CQ = nullptr;
};

} // namespace NKikimr::NGRpcService
