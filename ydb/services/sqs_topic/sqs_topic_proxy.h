#pragma once

#include <ydb/public/api/grpc/draft/ydb_sqs_topic_v1.pb.h>
#include <ydb/public/api/grpc/draft/ydb_ymq_v1.pb.h>

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>


namespace NKikimr::NSqsTopic {

    template <typename TReq, typename TResp, bool IsOperation>
    struct TYdbGrpcMethodAccessorTraits: public NKikimr::NGRpcService::TYdbGrpcMethodAccessorTraits<TReq, TResp, IsOperation> {
        // use an additional class to avoid collision with TGrpcRequestOperationCall specializations in different service that uses exactly the same parameters
    };

    template<class TRequest, class TResponse>
    using TGrpcRequestOperationCallAlais = NKikimr::NGRpcService::TGrpcRequestOperationCall<TRequest, TResponse, NKikimr::NGRpcService::NRuntimeEvents::EType::COMMON, NKikimr::NSqsTopic::TYdbGrpcMethodAccessorTraits<TRequest, TResponse, true>>;
}

namespace NKikimr::NGRpcService {

using TEvSqsTopicGetQueueUrlRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::GetQueueUrlRequest, Ydb::Ymq::V1::GetQueueUrlResponse>;
using TEvSqsTopicCreateQueueRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::CreateQueueRequest, Ydb::Ymq::V1::CreateQueueResponse>;
using TEvSqsTopicSendMessageRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::SendMessageRequest, Ydb::Ymq::V1::SendMessageResponse>;
using TEvSqsTopicReceiveMessageRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::ReceiveMessageRequest, Ydb::Ymq::V1::ReceiveMessageResponse>;
using TEvSqsTopicGetQueueAttributesRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::GetQueueAttributesRequest, Ydb::Ymq::V1::GetQueueAttributesResponse>;
using TEvSqsTopicListQueuesRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::ListQueuesRequest, Ydb::Ymq::V1::ListQueuesResponse>;
using TEvSqsTopicDeleteMessageRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::DeleteMessageRequest, Ydb::Ymq::V1::DeleteMessageResponse>;
using TEvSqsTopicPurgeQueueRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::PurgeQueueRequest, Ydb::Ymq::V1::PurgeQueueResponse>;
using TEvSqsTopicDeleteQueueRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::DeleteQueueRequest, Ydb::Ymq::V1::DeleteQueueResponse>;
using TEvSqsTopicChangeMessageVisibilityRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::ChangeMessageVisibilityRequest, Ydb::Ymq::V1::ChangeMessageVisibilityResponse>;
using TEvSqsTopicSetQueueAttributesRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::SetQueueAttributesRequest, Ydb::Ymq::V1::SetQueueAttributesResponse>;
using TEvSqsTopicSendMessageBatchRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::SendMessageBatchRequest, Ydb::Ymq::V1::SendMessageBatchResponse>;
using TEvSqsTopicDeleteMessageBatchRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::DeleteMessageBatchRequest, Ydb::Ymq::V1::DeleteMessageBatchResponse>;
using TEvSqsTopicChangeMessageVisibilityBatchRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequest, Ydb::Ymq::V1::ChangeMessageVisibilityBatchResponse>;
using TEvSqsTopicListDeadLetterSourceQueuesRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::ListDeadLetterSourceQueuesRequest, Ydb::Ymq::V1::ListDeadLetterSourceQueuesResponse>;
using TEvSqsTopicListQueueTagsRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::ListQueueTagsRequest, Ydb::Ymq::V1::ListQueueTagsResponse>;
using TEvSqsTopicTagQueueRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::TagQueueRequest, Ydb::Ymq::V1::TagQueueResponse>;
using TEvSqsTopicUntagQueueRequest = ::NKikimr::NSqsTopic::TGrpcRequestOperationCallAlais<Ydb::Ymq::V1::UntagQueueRequest, Ydb::Ymq::V1::UntagQueueResponse>;

}
