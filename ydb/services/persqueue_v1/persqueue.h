#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/core/grpc_services/service_topic.h>

namespace NKikimr {

namespace NGRpcService {
namespace V1 {

class TGRpcPersQueueService
    : public TGrpcServiceBase<Ydb::PersQueue::V1::PersQueueService>
{
public:
    TGRpcPersQueueService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const NActors::TActorId& schemeCache, const NActors::TActorId& grpcRequestProxy, bool rlAllowed);

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;
    void StopService() noexcept override;

    using NYdbGrpc::TGrpcServiceBase<Ydb::PersQueue::V1::PersQueueService>::GetService;


private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;

    NActors::TActorId SchemeCache;
    NKikimr::NGRpcProxy::V1::IClustersCfgProvider* ClustersCfgProvider = nullptr;
};

} // namespace V1
} // namespace NGRpcService
} // namespace NKikimr
