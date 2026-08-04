#pragma once

#include "yql_http_gateway.h"

#include <curl/curl.h>
#include <optional>
#include <unordered_set>

namespace NYql {

struct THttpRetryPolicyOptions {
    // If not set, default maxTime (5 minutes) is used
    std::optional<TDuration> MaxTime;
    size_t MaxRetries = std::numeric_limits<size_t>::max();
    std::unordered_set<CURLcode> RetriedCurlCodes;

    THttpRetryPolicyOptions();
    THttpRetryPolicyOptions(std::optional<TDuration> maxTime, size_t maxRetries = std::numeric_limits<size_t>::max());
};

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions&& options = {});

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime, size_t maxRetries = std::numeric_limits<size_t>::max()); // Zero means default maxTime

IHTTPGateway::TRetryPolicy::TPtr GetFqHTTPRetryPolicy();

}
