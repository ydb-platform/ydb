#pragma once

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NGRpcService {

class TEtcdKVService
    : public NYdbGrpc::TGrpcServiceBase<etcdserverpb::KV>
{
public:
    TEtcdKVService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId = {});

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem *const ActorSystem;
    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    grpc::ServerCompletionQueue* CQ = nullptr;
};

class TEtcdWatchService
    : public NYdbGrpc::TGrpcServiceBase<etcdserverpb::Watch>
{
public:
    TEtcdWatchService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId = {});

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem *const ActorSystem;
    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    grpc::ServerCompletionQueue* CQ = nullptr;
};

} // namespace NKikimr::NGRpcService
