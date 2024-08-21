#pragma once

#include <ydb/public/api/grpc/draft/ydb_ymq_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>


namespace NKikimr {
namespace NGRpcService {

using TEvYmqGetQueueUrlRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::GetQueueUrlRequest, Ydb::Ymq::V1::GetQueueUrlResponse>;
using TEvYmqCreateQueueRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::CreateQueueRequest, Ydb::Ymq::V1::CreateQueueResponse>;
using TEvYmqSendMessageRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::SendMessageRequest, Ydb::Ymq::V1::SendMessageResponse>;
using TEvYmqReceiveMessageRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::ReceiveMessageRequest, Ydb::Ymq::V1::ReceiveMessageResponse>;
using TEvYmqGetQueueAttributesRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::GetQueueAttributesRequest, Ydb::Ymq::V1::GetQueueAttributesResponse>;
using TEvYmqListQueuesRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::ListQueuesRequest, Ydb::Ymq::V1::ListQueuesResponse>;
using TEvYmqDeleteMessageRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::DeleteMessageRequest, Ydb::Ymq::V1::DeleteMessageResponse>;
using TEvYmqPurgeQueueRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::PurgeQueueRequest, Ydb::Ymq::V1::PurgeQueueResponse>;
using TEvYmqDeleteQueueRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::DeleteQueueRequest, Ydb::Ymq::V1::DeleteQueueResponse>;
using TEvYmqChangeMessageVisibilityRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::ChangeMessageVisibilityRequest, Ydb::Ymq::V1::ChangeMessageVisibilityResponse>;
using TEvYmqSetQueueAttributesRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::SetQueueAttributesRequest, Ydb::Ymq::V1::SetQueueAttributesResponse>;
using TEvYmqSendMessageBatchRequest = TGrpcRequestOperationCall<Ydb::Ymq::V1::SendMessageBatchRequest, Ydb::Ymq::V1::SendMessageBatchResponse>;

}
}
