#include "retry.h"

#include <util/stream/output.h>

void DoWithRetry(std::function<void()> func, TRetryOptions retryOptions) {
    DoWithRetry(func, retryOptions, true);
}

bool DoWithRetryOnRetCode(std::function<bool()> func, TRetryOptions retryOptions) {
    for (ui32 attempt = 0; attempt <= retryOptions.RetryCount; ++attempt) {
        if (func()) {
            return true;
        }
        auto sleep = retryOptions.SleepFunction;
        sleep(retryOptions.GetTimeToSleep(attempt));
    }
    return false;
}

TRetryOptions MakeRetryOptions(const NRetry::TRetryOptionsPB& retryOptions) {
    return TRetryOptions(retryOptions.GetMaxTries(),
                         TDuration::MilliSeconds(retryOptions.GetInitialSleepMs()),
                         TDuration::MilliSeconds(retryOptions.GetRandomDeltaMs()),
                         TDuration::MilliSeconds(retryOptions.GetSleepIncrementMs()),
                         TDuration::MilliSeconds(retryOptions.GetExponentalMultiplierMs()));
}
