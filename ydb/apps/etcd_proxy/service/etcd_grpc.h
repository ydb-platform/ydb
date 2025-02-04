#pragma once

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NGRpcService {

template<class TService>
class TEtcdServiceBase
    : public NYdbGrpc::TGrpcServiceBase<TService>
{
protected:
    TEtcdServiceBase(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : ActorSystem(actorSystem), Counters(std::move(counters))
    {}

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) final {
        CQ = cq;
        SetupIncomingRequests(std::move(logger));
    }

    virtual void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) = 0;

    NActors::TActorSystem *const ActorSystem;
    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    grpc::ServerCompletionQueue* CQ = nullptr;
};

class TEtcdKVService : public TEtcdServiceBase<etcdserverpb::KV>
{
public:
    TEtcdKVService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId = {});
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) final;
};

class TEtcdWatchService : public TEtcdServiceBase<etcdserverpb::Watch>
{
public:
    TEtcdWatchService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId = {});
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) final;
};

class TEtcdLeaseService : public TEtcdServiceBase<etcdserverpb::Lease>
{
public:
    TEtcdLeaseService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId = {});
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) final;
};

} // namespace NKikimr::NGRpcService
