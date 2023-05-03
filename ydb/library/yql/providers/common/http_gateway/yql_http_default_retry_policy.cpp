#include "yql_http_default_retry_policy.h"

namespace NYql {

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime, size_t maxRetries) {
    if (!maxTime) {
        maxTime = TDuration::Minutes(5);
    }
    return IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy([](CURLcode curlCode, long httpCode) {

        switch (curlCode) {
            case CURLE_OK:
                // look to http code
                break;
            case CURLE_COULDNT_CONNECT:
            case CURLE_WEIRD_SERVER_REPLY:
            case CURLE_WRITE_ERROR:
            case CURLE_READ_ERROR:
            case CURLE_OPERATION_TIMEDOUT:
            case CURLE_SSL_CONNECT_ERROR:
            case CURLE_BAD_DOWNLOAD_RESUME:
            case CURLE_SEND_ERROR:
            case CURLE_RECV_ERROR:
            case CURLE_NO_CONNECTION_AVAILABLE:
                // retry small number of known errors
                return ERetryErrorClass::ShortRetry;
            default:
                // do not retry others
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

}
