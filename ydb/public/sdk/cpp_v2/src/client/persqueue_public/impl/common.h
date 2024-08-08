#pragma once

#include <ydb-cpp-sdk/client/types/status_codes.h>

#include <ydb-cpp-sdk/library/retry/retry_policy.h>

namespace NYdb::NPersQueue {
    ERetryErrorClass GetRetryErrorClass(EStatus status);
    ERetryErrorClass GetRetryErrorClassV2(EStatus status);
}
