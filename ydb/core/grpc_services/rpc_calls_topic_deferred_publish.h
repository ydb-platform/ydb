#pragma once

#include <ydb/public/api/protos/ydb_topic_deferred_publish.pb.h>

#include "rpc_calls.h"

namespace NKikimr::NGRpcService {

using TEvBeginPublicationRequest = TGrpcRequestOperationCall<
    Ydb::Topic::DeferredPublish::BeginPublicationRequest,
    Ydb::Topic::DeferredPublish::BeginPublicationResponse>;
using TEvPublishRequest = TGrpcRequestOperationCall<
    Ydb::Topic::DeferredPublish::PublishRequest,
    Ydb::Topic::DeferredPublish::PublishResponse>;
using TEvCancelPublicationRequest = TGrpcRequestOperationCall<
    Ydb::Topic::DeferredPublish::CancelPublicationRequest,
    Ydb::Topic::DeferredPublish::CancelPublicationResponse>;
using TEvListPublicationsRequest = TGrpcRequestOperationCall<
    Ydb::Topic::DeferredPublish::ListPublicationsRequest,
    Ydb::Topic::DeferredPublish::ListPublicationsResponse>;
using TEvDescribePublicationRequest = TGrpcRequestOperationCall<
    Ydb::Topic::DeferredPublish::DescribePublicationRequest,
    Ydb::Topic::DeferredPublish::DescribePublicationResponse>;

}
