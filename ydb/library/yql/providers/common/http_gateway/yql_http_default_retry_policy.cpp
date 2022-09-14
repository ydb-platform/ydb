#include "yql_http_default_retry_policy.h"

namespace NYql {

IRetryPolicy<long>::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime) {
    if (!maxTime) {
        maxTime = TDuration::Minutes(5);
    }
    return IRetryPolicy<long>::GetExponentialBackoffPolicy([](long httpCode) {
        switch (httpCode) {
            case 0:
                return ERetryErrorClass::ShortRetry;
            case 408: // Request Timeout
            case 425: // Too Early
            case 429: // Too Many Requests
            case 500: // Internal Server Error
            case 502: // Bad Gateway
            case 503: // Service Unavailable
            case 504: // Gateway Timeout
                return ERetryErrorClass::LongRetry;
            default:
                return ERetryErrorClass::NoRetry;
        }
    },
    TDuration::MilliSeconds(10), // minDelay
    TDuration::MilliSeconds(200), // minLongRetryDelay
    TDuration::Seconds(30), // maxDelay
    std::numeric_limits<size_t>::max(), // maxRetries
    maxTime); // maxTime
}

}
