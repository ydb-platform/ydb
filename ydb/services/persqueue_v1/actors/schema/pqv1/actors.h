#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/services/persqueue_v1/actors/events.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

NActors::IActor* CreateDropTopicActor(NGRpcService::IRequestOpCtx* request);

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
