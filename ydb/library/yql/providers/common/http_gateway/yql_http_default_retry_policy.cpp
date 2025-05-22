#include "yql_http_default_retry_policy.h"

namespace NYql {

std::unordered_set<CURLcode> YqlRetriedCurlCodes() {
    return {
        CURLE_COULDNT_CONNECT,
        CURLE_WEIRD_SERVER_REPLY,
        CURLE_WRITE_ERROR,
        CURLE_READ_ERROR,
        CURLE_OPERATION_TIMEDOUT,
        CURLE_SSL_CONNECT_ERROR,
        CURLE_BAD_DOWNLOAD_RESUME,
        CURLE_SEND_ERROR,
        CURLE_RECV_ERROR,
        CURLE_NO_CONNECTION_AVAILABLE
    };
}

std::unordered_set<CURLcode> FqRetriedCurlCodes() {
    return {
        CURLE_COULDNT_CONNECT,
        CURLE_WEIRD_SERVER_REPLY,
        CURLE_WRITE_ERROR,
        CURLE_READ_ERROR,
        CURLE_OPERATION_TIMEDOUT,
        CURLE_SSL_CONNECT_ERROR,
        CURLE_BAD_DOWNLOAD_RESUME,
        CURLE_SEND_ERROR,
        CURLE_RECV_ERROR,
        CURLE_NO_CONNECTION_AVAILABLE,
        CURLE_GOT_NOTHING,
        CURLE_COULDNT_RESOLVE_HOST
    };
}

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions&& options) {
    auto maxTime = options.MaxTime;
    auto maxRetries = options.MaxRetries;
    if (!maxTime) {
        maxTime = TDuration::Minutes(5);
    }
    return IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy([options = std::move(options)](CURLcode curlCode, long httpCode) {
        if (curlCode == CURLE_OK) {
            // pass
        } else if (options.RetriedCurlCodes.contains(curlCode)) {
            return ERetryErrorClass::ShortRetry;
        } else {
            return ERetryErrorClass::NoRetry;
        }

        switch (httpCode) {
            case 0:
                // rare case when curl code is not available like manual cancelling, not retriable anymore
                return ERetryErrorClass::NoRetry;
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
    maxRetries, // maxRetries
    maxTime); // maxTime
}

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime, size_t maxRetries) {
    return GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{.MaxTime = maxTime, .MaxRetries = maxRetries});
}

}
