#pragma once

#include <util/generic/fwd.h>

namespace NActors {
class IActor;
}

namespace NKikimr::NGRpcService {
class IRequestOpCtx;
}

namespace NKikimr::NGRpcProxy::V1::NTopic {

NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateDropTopicActor(NGRpcService::IRequestOpCtx* request);

} // namespace NKikimr::NGRpcProxy::V1::NTopic
