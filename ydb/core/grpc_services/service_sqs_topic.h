#pragma once
#include <memory>

namespace NActors {
    struct TActorId;
}

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoSqsTopicGetQueueUrlRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicCreateQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicSendMessageRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicReceiveMessageRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicGetQueueAttributesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicListQueuesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicDeleteMessageRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicPurgeQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicDeleteQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicChangeMessageVisibilityRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicSetQueueAttributesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicSendMessageBatchRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicDeleteMessageBatchRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicChangeMessageVisibilityBatchRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicListDeadLetterSourceQueuesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicListQueueTagsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicTagQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
    void DoSqsTopicUntagQueueRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
}
