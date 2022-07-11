#pragma once

#include <ydb/public/api/grpc/draft/ydb_logstore_v1.grpc.pb.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <library/cpp/actors/core/actorsystem.h>

namespace NKikimr::NGRpcService {

class TGRpcYdbLogStoreService
    : public NGrpc::TGrpcServiceBase<Ydb::LogStore::V1::LogStoreService>
{
public:
    TGRpcYdbLogStoreService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        NActors::TActorId id);

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();
private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
    NGrpc::TGlobalLimiter* Limiter_ = nullptr;
};

}
