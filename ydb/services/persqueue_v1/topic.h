#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/service_topic.h>

namespace NKikimr::NGRpcService::V1 {

class TGRpcTopicService
    : public TGrpcServiceBase<Ydb::Topic::V1::TopicService>
{
public:
    TGRpcTopicService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache, const NActors::TActorId& grpcRequestProxy, bool rlAllowed);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void StopService() noexcept override;

    using NYdbGrpc::TGrpcServiceBase<Ydb::Topic::V1::TopicService>::GetService;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;

    static void DoUpdateOffsetsInTransaction(std::unique_ptr<IRequestOpCtx> p,
                                             const IFacilityProvider &);

    NActors::TActorId SchemeCache;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    NKikimr::NGRpcProxy::V1::IClustersCfgProvider* ClustersCfgProvider = nullptr;
};

} // namespace NKikimr::NGRpcService::V1
