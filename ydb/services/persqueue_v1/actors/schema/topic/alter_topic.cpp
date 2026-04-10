#include "actors.h"
#include "alter_topic.h"

namespace NKikimr::NGRpcProxy::V1::NTopic {

NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request) {
    return new TAlterTopicActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic