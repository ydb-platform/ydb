#pragma once

#include <library/cpp/retry/retry_policy.h>

namespace NYql {

IRetryPolicy<long>::TPtr GetHTTPDefaultRetryPolicy();

}
