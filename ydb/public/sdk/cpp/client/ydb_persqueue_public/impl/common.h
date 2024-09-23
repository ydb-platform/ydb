#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <library/cpp/retry/retry_policy.h>

namespace NYdb::NPersQueue {
    ERetryErrorClass GetRetryErrorClass(EStatus status);
    ERetryErrorClass GetRetryErrorClassV2(EStatus status);
}
