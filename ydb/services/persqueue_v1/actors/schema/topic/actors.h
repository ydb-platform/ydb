#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/services/persqueue_v1/actors/events.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request);

NActors::IActor* CreateAlterTopicInternalActor(
    NKikimr::NGRpcProxy::V1::TAlterTopicRequest&& request,
    NThreading::TPromise<NKikimr::NGRpcProxy::V1::TAlterTopicResponse>&& promise
);

} // namespace NKikimr::NGRpcProxy::V1::NTopic
