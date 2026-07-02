#pragma once

#include <ydb/core/grpc_services/base/base_service.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/api/grpc/ydb_topic_deferred_publish_v1.grpc.pb.h>

namespace NKikimr::NGRpcService::V1 {

class TGRpcTopicDeferredPublishService
    : public TGrpcServiceBase<Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService>
{
public:
    using TGrpcServiceBase<Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService>::TGrpcServiceBase;

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) override;

private:
    void SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) override;
};

}
