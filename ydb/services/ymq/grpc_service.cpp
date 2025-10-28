#include "grpc_service.h"

#include <ydb/core/grpc_services/service_ymq.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>


namespace NKikimr::NGRpcService {

void TGRpcYmqService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Ymq::V1;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_YMQ_METHOD
#error SETUP_YMQ_METHOD macro already defined
#endif

#define SETUP_YMQ_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_RUNTIME_EVENT_METHOD(Y_CAT(Ymq, methodName), \
        Y_CAT(methodName, Request), \
        Y_CAT(methodName, Response), \
        methodCallback, \
        rlMode, \
        requestType, \
        YDB_API_DEFAULT_COUNTER_BLOCK(ymq, methodName), \
        auditMode, \
        COMMON, \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall, \
        GRpcRequestProxyId_, \
        CQ_, \
        nullptr, \
        nullptr)

    SETUP_YMQ_METHOD(GetQueueUrl, DoYmqGetQueueUrlRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_YMQ_METHOD(CreateQueue, DoYmqCreateQueueRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_YMQ_METHOD(SendMessage, DoYmqSendMessageRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_YMQ_METHOD(ReceiveMessage, DoYmqReceiveMessageRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_YMQ_METHOD(GetQueueAttributes, DoYmqGetQueueAttributesRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_YMQ_METHOD(ListQueues, DoYmqListQueuesRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_YMQ_METHOD(DeleteMessage, DoYmqDeleteMessageRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_YMQ_METHOD(PurgeQueue, DoYmqPurgeQueueRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_YMQ_METHOD(DeleteQueue, DoYmqDeleteQueueRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_YMQ_METHOD(ChangeMessageVisibility, DoYmqChangeMessageVisibilityRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_YMQ_METHOD(SetQueueAttributes, DoYmqSetQueueAttributesRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_YMQ_METHOD(SendMessageBatch, DoYmqSendMessageBatchRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_YMQ_METHOD(DeleteMessageBatch, DoYmqDeleteMessageBatchRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_YMQ_METHOD(ChangeMessageVisibilityBatch, DoYmqChangeMessageVisibilityBatchRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml));
    SETUP_YMQ_METHOD(ListDeadLetterSourceQueues, DoYmqListDeadLetterSourceQueuesRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_YMQ_METHOD(ListQueueTags, DoYmqListQueueTagsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_YMQ_METHOD(TagQueue, DoYmqTagQueueRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    SETUP_YMQ_METHOD(UntagQueue, DoYmqUntagQueueRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));

#undef SETUP_YMQ_METHOD
}

}
