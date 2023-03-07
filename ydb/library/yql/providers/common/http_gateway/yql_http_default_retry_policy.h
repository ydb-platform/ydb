#pragma once

#include <library/cpp/retry/retry_policy.h>

namespace NYql {

IRetryPolicy<long>::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime = TDuration::Zero(), size_t maxRetries = std::numeric_limits<size_t>::max()); // Zero means default maxTime

}
