#pragma once

#include <ydb/public/api/grpc/ydb_keyvalue_v2.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/actorid.h>


namespace NKikimr::NGRpcService {

class TKeyValueGRpcServiceV2
        : public NYdbGrpc::TGrpcServiceBase<Ydb::KeyValue::V2::KeyValueService>
{
public:
    TKeyValueGRpcServiceV2(NActors::TActorSystem* actorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            NActors::TActorId grpcRequestProxyId);
    ~TKeyValueGRpcServiceV2();

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
