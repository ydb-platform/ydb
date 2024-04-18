#pragma once

#include "yql_http_gateway.h"

#include <curl/curl.h>
#include <unordered_set>

namespace NYql {

std::unordered_set<CURLcode> YqlRetriedCurlCodes();
std::unordered_set<CURLcode> FqRetriedCurlCodes();

struct THttpRetryPolicyOptions {
    TDuration MaxTime = TDuration::Zero(); // Zero means default maxTime
    size_t MaxRetries = std::numeric_limits<size_t>::max();
    std::unordered_set<CURLcode> RetriedCurlCodes = YqlRetriedCurlCodes();
};

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions&& options = {});

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime, size_t maxRetries = std::numeric_limits<size_t>::max()); // Zero means default maxTime

}
