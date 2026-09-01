#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

namespace NKikimr::NGRpcProxy::V1 {

struct TGetPartitionsLocationRequest;

namespace NTopic {

void ResolveTopicRequestPaths(Ydb::Topic::CreateTopicRequest& request, const TMaybe<TString>& database);
void ResolveTopicRequestPaths(Ydb::Topic::AlterTopicRequest& request, const TMaybe<TString>& database);

NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateCreateTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateDescribeConsumerActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateDescribePartitionActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateDescribeTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateDropTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreatePartitionsLocationActor(const TActorId& requester, const TGetPartitionsLocationRequest& request);

} // namespace NTopic
} // namespace NKikimr::NGRpcProxy::V1
