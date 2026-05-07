#pragma once

#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

NActors::IActor* CreateAddConsumerActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateCreateTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateRemoveConsumerActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateDropTopicActor(NGRpcService::IRequestOpCtx* request);

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
