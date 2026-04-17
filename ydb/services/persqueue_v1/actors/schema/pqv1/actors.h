#pragma once

#include <util/generic/fwd.h>

namespace NActors {
class IActor;
}

namespace NKikimr::NGRpcService {
class IRequestOpCtx;
}

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request, const TString& localCluster);
NActors::IActor* CreateDropTopicActor(NGRpcService::IRequestOpCtx* request);

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
