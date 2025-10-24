#include "grpc_service.h"

#include <ydb/core/grpc_services/service_sqs_topic.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_board/cache.h>


namespace NKikimr::NGRpcService {

void TGRpcSqsTopicService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger)
{
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif

#define ADD_REQUEST(NAME, CB, ATTR, LIMIT_TYPE) \
    MakeIntrusive<TGRpcRequest<Ydb::SqsTopic::V1::NAME##Request, Ydb::SqsTopic::V1::NAME##Response, TGRpcSqsTopicService>> \
        (this, &Service_, CQ_,                                                                                             \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                                   \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                           \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                                    \
                    new TGrpcRequestOperationCall<Ydb::SqsTopic::V1::NAME##Request, Ydb::SqsTopic::V1::NAME##Response>     \
                        (ctx, CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::LIMIT_TYPE), ATTR}));                     \
            }, &Ydb::SqsTopic::V1::SqsTopicService::AsyncService::RequestSqsTopic ## NAME,                                 \
            #NAME, logger, getCounterBlock("sqs_topic", #NAME))->Run();

    ADD_REQUEST(GetQueueUrl, DoSqsTopicGetQueueUrlRequest, nullptr, Off)
    ADD_REQUEST(CreateQueue, DoSqsTopicCreateQueueRequest, nullptr, Off)
    ADD_REQUEST(SendMessage, DoSqsTopicSendMessageRequest, nullptr, Off)
    ADD_REQUEST(ReceiveMessage, DoSqsTopicReceiveMessageRequest, nullptr, Off)
    ADD_REQUEST(GetQueueAttributes, DoSqsTopicGetQueueAttributesRequest, nullptr, Off)
    ADD_REQUEST(ListQueues, DoSqsTopicListQueuesRequest, nullptr, Off)
    ADD_REQUEST(DeleteMessage, DoSqsTopicDeleteMessageRequest, nullptr, Off)
    ADD_REQUEST(PurgeQueue, DoSqsTopicPurgeQueueRequest, nullptr, Off)
    ADD_REQUEST(DeleteQueue, DoSqsTopicDeleteQueueRequest, nullptr, Off)
    ADD_REQUEST(ChangeMessageVisibility, DoSqsTopicChangeMessageVisibilityRequest, nullptr, Off)
    ADD_REQUEST(SetQueueAttributes, DoSqsTopicSetQueueAttributesRequest, nullptr, Off)
    ADD_REQUEST(SendMessageBatch, DoSqsTopicSendMessageBatchRequest, nullptr, Off)
    ADD_REQUEST(DeleteMessageBatch, DoSqsTopicDeleteMessageBatchRequest, nullptr, Off)
    ADD_REQUEST(ChangeMessageVisibilityBatch, DoSqsTopicChangeMessageVisibilityBatchRequest, nullptr, Off)
    ADD_REQUEST(ListDeadLetterSourceQueues, DoSqsTopicListDeadLetterSourceQueuesRequest, nullptr, Off)
    ADD_REQUEST(ListQueueTags, DoSqsTopicListQueueTagsRequest, nullptr, Off)
    ADD_REQUEST(TagQueue, DoSqsTopicTagQueueRequest, nullptr, Off)
    ADD_REQUEST(UntagQueue, DoSqsTopicUntagQueueRequest, nullptr, Off)

#undef ADD_REQUEST
}

}
