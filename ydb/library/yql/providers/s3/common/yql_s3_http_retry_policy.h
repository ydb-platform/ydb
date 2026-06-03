#pragma once

#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>

#include <curl/curl.h>
#include <unordered_set>

namespace NYql {

IHTTPGateway::TRetryPolicy::TPtr GetFqS3HttpRetryPolicy();

}
