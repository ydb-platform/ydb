#pragma once
#include <memory>

namespace NActors {
struct TActorId;
}

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoYmqGetQueueUrlRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqCreateQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqSendMessageRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqReceiveMessageRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqGetQueueAttributesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqListQueuesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqDeleteMessageRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqPurgeQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqDeleteQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoYmqChangeMessageVisibilityRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
}
}
