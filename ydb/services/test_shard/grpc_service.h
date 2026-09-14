#pragma once

#include <ydb/public/api/grpc/ydb_test_shard_v1.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>


namespace NKikimr::NGRpcService {

class TTestShardSetGRpcService
        : public NYdbGrpc::TGrpcServiceBase<Ydb::TestShardSet::V1::TestShardSetService>
{
public:
    TTestShardSetGRpcService(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId);
    ~TTestShardSetGRpcService();

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

private:
    NActors::TActorSystem* ActorSystem_ = nullptr;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;

    grpc::ServerCompletionQueue* CQ_ = nullptr;
};

} // namespace NKikimr::NGRpcService
