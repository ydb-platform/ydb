#pragma once

#include "ydb/public/api/grpc/ydb_table_v1.grpc.pb.h"
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/library/grpc/server/grpc_server.h>

#include <ydb/public/api/grpc/draft/fq_v1.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TGRpcYdbOverFqService
    : public NYdbGrpc::TGrpcServiceBase<Ydb::Table::V1::TableService>
{
public:
    TGRpcYdbOverFqService(NActors::TActorSystem* system, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        NActors::TActorId id);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) override;

    bool IncRequest();
    void DecRequest();
private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger);

    NActors::TActorSystem* ActorSystem_;
    grpc::ServerCompletionQueue* CQ_ = nullptr;

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters_;
    NActors::TActorId GRpcRequestProxyId_;
    NYdbGrpc::TGlobalLimiter* Limiter_ = nullptr;
};

} // namespace NGRpcService
} // namespace NKikimr
