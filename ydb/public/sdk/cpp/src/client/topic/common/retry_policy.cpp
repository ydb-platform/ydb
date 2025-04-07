#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/errors.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/retry_policy.h>

namespace NYdb::inline Dev::NTopic {

IRetryPolicy::TPtr IRetryPolicy::GetDefaultPolicy() {
    static IRetryPolicy::TPtr policy = GetExponentialBackoffPolicy();
    return policy;
}

IRetryPolicy::TPtr IRetryPolicy::GetNoRetryPolicy() {
    return ::IRetryPolicy<EStatus>::GetNoRetryPolicy();
}

IRetryPolicy::TPtr
IRetryPolicy::GetExponentialBackoffPolicy(TDuration minDelay, TDuration minLongRetryDelay, TDuration maxDelay,
                                        size_t maxRetries, TDuration maxTime, double scaleFactor,
                                        std::function<ERetryErrorClass(EStatus)> customRetryClassFunction) {
    return ::IRetryPolicy<EStatus>::GetExponentialBackoffPolicy(
        customRetryClassFunction ? customRetryClassFunction : GetRetryErrorClass, minDelay,
        minLongRetryDelay, maxDelay, maxRetries, maxTime, scaleFactor);
}

IRetryPolicy::TPtr
IRetryPolicy::GetFixedIntervalPolicy(TDuration delay, TDuration longRetryDelay, size_t maxRetries, TDuration maxTime,
                                    std::function<ERetryErrorClass(EStatus)> customRetryClassFunction) {
    return ::IRetryPolicy<EStatus>::GetFixedIntervalPolicy(
        customRetryClassFunction ? customRetryClassFunction : GetRetryErrorClass, delay,
        longRetryDelay, maxRetries, maxTime);
}

}  // namespace NYdb::NTopic