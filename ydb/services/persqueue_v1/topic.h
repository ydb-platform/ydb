#pragma once

#include <library/cpp/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <library/cpp/grpc/server/grpc_server.h>

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/core/grpc_services/base/base.h>


namespace NKikimr::NGRpcService::V1 {

class TGRpcTopicService
    : public TGrpcServiceBase<Ydb::Topic::V1::TopicService>
{
public:
    TGRpcTopicService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache, const NActors::TActorId& grpcRequestProxy, bool rlAllowed);

    void InitService(grpc::ServerCompletionQueue* cq, NGrpc::TLoggerPtr logger) override;
    void StopService() noexcept override;

    using NGrpc::TGrpcServiceBase<Ydb::Topic::V1::TopicService>::GetService;

private:
    void SetupIncomingRequests(NGrpc::TLoggerPtr logger) override;

    static void DoUpdateOffsetsInTransaction(std::unique_ptr<IRequestOpCtx> p,
                                             const IFacilityProvider &);

    NActors::TActorId SchemeCache;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
};

} // namespace NKikimr::NGRpcService::V1
