#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

namespace NKikimr {
namespace NGRpcService {

using TEvCreateRateLimiterResource = TGrpcRequestOperationCall<Ydb::RateLimiter::CreateResourceRequest,
    Ydb::RateLimiter::CreateResourceResponse>;
using TEvAlterRateLimiterResource = TGrpcRequestOperationCall<Ydb::RateLimiter::AlterResourceRequest,
    Ydb::RateLimiter::AlterResourceResponse>;
using TEvDropRateLimiterResource = TGrpcRequestOperationCall<Ydb::RateLimiter::DropResourceRequest,
    Ydb::RateLimiter::DropResourceResponse>;
using TEvListRateLimiterResources = TGrpcRequestOperationCall<Ydb::RateLimiter::ListResourcesRequest,
    Ydb::RateLimiter::ListResourcesResponse>;
using TEvDescribeRateLimiterResource = TGrpcRequestOperationCall<Ydb::RateLimiter::DescribeResourceRequest,
    Ydb::RateLimiter::DescribeResourceResponse>;
using TEvAcquireRateLimiterResource = TGrpcRequestOperationCall<Ydb::RateLimiter::AcquireResourceRequest,
    Ydb::RateLimiter::AcquireResourceResponse>;

}
}
