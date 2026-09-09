#pragma once

#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NGRpcProxy::V1 {

NActors::IActor* CreateSetOffsetsActor(NGRpcService::IRequestOpCtx* request);

} // namespace NKikimr::NGRpcProxy::V1
