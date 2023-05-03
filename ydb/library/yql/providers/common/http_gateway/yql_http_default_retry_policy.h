#pragma once

#include "yql_http_gateway.h"

namespace NYql {

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime = TDuration::Zero(), size_t maxRetries = std::numeric_limits<size_t>::max()); // Zero means default maxTime

}
