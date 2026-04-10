#pragma once

#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request);

} // namespace NKikimr::NGRpcProxy::V1::NTopic
