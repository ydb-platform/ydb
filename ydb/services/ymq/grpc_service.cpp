#include "grpc_service.h"

#include <ydb/core/grpc_services/service_ymq.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_board/cache.h>


namespace NKikimr::NGRpcService {

void TGRpcYmqService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger)
{
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using std::placeholders::_1;
    using std::placeholders::_2;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
//TODO: убрать дублирование с DataStreams
#define ADD_REQUEST(NAME, CB, ATTR, LIMIT_TYPE) \
    MakeIntrusive<TGRpcRequest<Ydb::Ymq::V1::NAME##Request, Ydb::Ymq::V1::NAME##Response, TGRpcYmqService>> \
        (this, &Service_, CQ_,                                                                                                      \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                                               \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                                    \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                                             \
                    new TGrpcRequestOperationCall<Ydb::Ymq::V1::NAME##Request, Ydb::Ymq::V1::NAME##Response>        \
                        (ctx, CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::LIMIT_TYPE), ATTR}));                              \
            }, &Ydb::Ymq::V1::YmqService::AsyncService::Request ## NAME,                                            \
            #NAME, logger, getCounterBlock("ymq", #NAME))->Run();

    ADD_REQUEST(GetQueueUrl, DoYmqGetQueueUrlRequest, nullptr, Off)
    ADD_REQUEST(CreateQueue, DoYmqCreateQueueRequest, nullptr, Off)
    ADD_REQUEST(SendMessage, DoYmqSendMessageRequest, nullptr, Off)
    ADD_REQUEST(ReceiveMessage, DoYmqReceiveMessageRequest, nullptr, Off)
    ADD_REQUEST(GetQueueAttributes, DoYmqGetQueueAttributesRequest, nullptr, Off)
    ADD_REQUEST(ListQueues, DoYmqListQueuesRequest, nullptr, Off)
    ADD_REQUEST(DeleteMessage, DoYmqDeleteMessageRequest, nullptr, Off)
    ADD_REQUEST(PurgeQueue, DoYmqPurgeQueueRequest, nullptr, Off)
    ADD_REQUEST(DeleteQueue, DoYmqDeleteQueueRequest, nullptr, Off)
    ADD_REQUEST(ChangeMessageVisibility, DoYmqChangeMessageVisibilityRequest, nullptr, Off)
    ADD_REQUEST(SetQueueAttributes, DoYmqSetQueueAttributesRequest, nullptr, Off)
    ADD_REQUEST(SendMessageBatch, DoYmqSendMessageBatchRequest, nullptr, Off)

#undef ADD_REQUEST
}

}
