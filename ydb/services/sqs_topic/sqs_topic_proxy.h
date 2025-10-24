#pragma once

#include <ydb/public/api/grpc/draft/ydb_sqs_topic_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>


namespace NKikimr {
namespace NGRpcService {

using TEvSqsTopicGetQueueUrlRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::GetQueueUrlRequest, Ydb::SqsTopic::V1::GetQueueUrlResponse>;
using TEvSqsTopicCreateQueueRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::CreateQueueRequest, Ydb::SqsTopic::V1::CreateQueueResponse>;
using TEvSqsTopicSendMessageRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::SendMessageRequest, Ydb::SqsTopic::V1::SendMessageResponse>;
using TEvSqsTopicReceiveMessageRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::ReceiveMessageRequest, Ydb::SqsTopic::V1::ReceiveMessageResponse>;
using TEvSqsTopicGetQueueAttributesRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::GetQueueAttributesRequest, Ydb::SqsTopic::V1::GetQueueAttributesResponse>;
using TEvSqsTopicListQueuesRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::ListQueuesRequest, Ydb::SqsTopic::V1::ListQueuesResponse>;
using TEvSqsTopicDeleteMessageRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::DeleteMessageRequest, Ydb::SqsTopic::V1::DeleteMessageResponse>;
using TEvSqsTopicPurgeQueueRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::PurgeQueueRequest, Ydb::SqsTopic::V1::PurgeQueueResponse>;
using TEvSqsTopicDeleteQueueRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::DeleteQueueRequest, Ydb::SqsTopic::V1::DeleteQueueResponse>;
using TEvSqsTopicChangeMessageVisibilityRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::ChangeMessageVisibilityRequest, Ydb::SqsTopic::V1::ChangeMessageVisibilityResponse>;
using TEvSqsTopicSetQueueAttributesRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::SetQueueAttributesRequest, Ydb::SqsTopic::V1::SetQueueAttributesResponse>;
using TEvSqsTopicSendMessageBatchRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::SendMessageBatchRequest, Ydb::SqsTopic::V1::SendMessageBatchResponse>;
using TEvSqsTopicDeleteMessageBatchRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::DeleteMessageBatchRequest, Ydb::SqsTopic::V1::DeleteMessageBatchResponse>;
using TEvSqsTopicChangeMessageVisibilityBatchRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::ChangeMessageVisibilityBatchRequest, Ydb::SqsTopic::V1::ChangeMessageVisibilityBatchResponse>;
using TEvSqsTopicListDeadLetterSourceQueuesRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::ListDeadLetterSourceQueuesRequest, Ydb::SqsTopic::V1::ListDeadLetterSourceQueuesResponse>;
using TEvSqsTopicListQueueTagsRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::ListQueueTagsRequest, Ydb::SqsTopic::V1::ListQueueTagsResponse>;
using TEvSqsTopicTagQueueRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::TagQueueRequest, Ydb::SqsTopic::V1::TagQueueResponse>;
using TEvSqsTopicUntagQueueRequest = TGrpcRequestOperationCall<Ydb::SqsTopic::V1::UntagQueueRequest, Ydb::SqsTopic::V1::UntagQueueResponse>;


}
}
