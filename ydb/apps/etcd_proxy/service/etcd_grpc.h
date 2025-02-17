#pragma once

#include "etcd_shared.h"

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NEtcd {

class IRequestCtx : public NKikimr::NGRpcService::IRequestCtx {
public:
    virtual void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details) = 0;
};

template<class TService>
class TEtcdServiceBase
    : public NYdbGrpc::TGrpcServiceBase<TService>
{
public:
    TEtcdServiceBase(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters, NActors::TActorId watchtower, TSharedStuff::TPtr stuff)
        : ActorSystem(actorSystem), Counters(std::move(counters)), Watchtower(std::move(watchtower)), Stuff(std::move(stuff))
    {}

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
        CQ = cq;
        SetupIncomingRequests(std::move(logger));
    }
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem *const ActorSystem;
    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    const NActors::TActorId Watchtower;
    const TSharedStuff::TPtr Stuff;
    grpc::ServerCompletionQueue* CQ = nullptr;
};

using TEtcdKVService = TEtcdServiceBase<etcdserverpb::KV>;
using TEtcdWatchService = TEtcdServiceBase<etcdserverpb::Watch>;
using TEtcdLeaseService = TEtcdServiceBase<etcdserverpb::Lease>;

} // namespace NEtcd
