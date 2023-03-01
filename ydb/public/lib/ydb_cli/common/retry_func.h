#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <util/datetime/base.h>

namespace NYdb::NConsoleClient {

void ExponentialBackoff(TDuration& sleep, TDuration max = TDuration::Minutes(5));

// function must return TStatus or derived struct
// function must be re-invokable
template <typename TFunction>
decltype(auto) RetryFunction(TFunction func, ui32 maxRetries = 10, TDuration retrySleep = TDuration::MilliSeconds(500)) {
    for (ui32 retryNumber = 0; retryNumber <= maxRetries; ++retryNumber) {
        auto result = func();

        if (result.IsSuccess()) {
            return result;
        }

        if (retryNumber == maxRetries) {
            return result;
        }

        switch (result.GetStatus()) {
            case EStatus::ABORTED:
                break;

            case EStatus::UNAVAILABLE:
            case EStatus::OVERLOADED:
            case EStatus::TRANSPORT_UNAVAILABLE:
            case EStatus::CLIENT_RESOURCE_EXHAUSTED:
                ExponentialBackoff(retrySleep);
                break;

            default:
                return result;
        }
    }

    // unreachable
    return func();
}

}
