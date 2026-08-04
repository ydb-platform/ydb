#include "yql_http_default_retry_policy.h"

namespace NYql {

namespace {

constexpr TDuration DEFAULT_MAX_TIME = TDuration::Minutes(5);
constexpr TDuration DNS_ERROR_MAX_TIME = TDuration::Seconds(10);

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
        CURLE_PARTIAL_FILE,
        CURLE_NO_CONNECTION_AVAILABLE,
        CURLE_GOT_NOTHING,
        CURLE_COULDNT_RESOLVE_HOST
    };
}

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

// Returns the retry error class for a curl/http error pair, shared by both policies.
ERetryErrorClass ClassifyError(CURLcode curlCode, long httpCode, const std::unordered_set<CURLcode>& retriedCurlCodes) {
    if (curlCode != CURLE_OK) {
        return retriedCurlCodes.contains(curlCode) ? ERetryErrorClass::ShortRetry : ERetryErrorClass::NoRetry;
    }
    switch (httpCode) {
        case 0:   return ERetryErrorClass::NoRetry; // manual cancelling
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
}

} // namespace

THttpRetryPolicyOptions::THttpRetryPolicyOptions()
    : RetriedCurlCodes(YqlRetriedCurlCodes())
{}

THttpRetryPolicyOptions::THttpRetryPolicyOptions(std::optional<TDuration> maxTime, size_t maxRetries)
    : MaxTime(maxTime)
    , MaxRetries(maxRetries)
    , RetriedCurlCodes(YqlRetriedCurlCodes())
{}

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions&& options) {
    auto maxTime = options.MaxTime.value_or(DEFAULT_MAX_TIME);
    auto maxRetries = options.MaxRetries;
    return IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy(
        [options = std::move(options)](CURLcode curlCode, long httpCode) {
            return ClassifyError(curlCode, httpCode, options.RetriedCurlCodes);
        },
        TDuration::MilliSeconds(10),  // minDelay
        TDuration::MilliSeconds(200), // minLongRetryDelay
        TDuration::Seconds(30),       // maxDelay
        maxRetries,
        maxTime);
}

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime, size_t maxRetries) {
    return GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{maxTime ? std::make_optional(maxTime) : std::nullopt, maxRetries});
}

// Custom policy for FQ: applies a shorter 10-second time budget for DNS resolution errors,
// while other retriable errors use the default 5-minute budget.
// A plain lambda in GetExponentialBackoffPolicy cannot do this because maxTime is a single
// value fixed at construction time — it cannot vary per error code within one retry session.
IHTTPGateway::TRetryPolicy::TPtr GetFqHTTPRetryPolicy() {
    struct TFqRetryState : IHTTPGateway::TRetryPolicy::IRetryState {
        explicit TFqRetryState(std::unordered_set<CURLcode> codes) : RetriedCurlCodes(std::move(codes)) {}

        TMaybe<TDuration> GetNextRetryDelay(CURLcode curlCode, long httpCode) override {
            if (!StartTime) {
                StartTime = TInstant::Now();
            }
            auto retryClass = ClassifyError(curlCode, httpCode, RetriedCurlCodes);
            if (retryClass == ERetryErrorClass::NoRetry) {
                return Nothing();
            }
            TDuration maxTime = (curlCode == CURLE_COULDNT_RESOLVE_HOST) ? DNS_ERROR_MAX_TIME : DEFAULT_MAX_TIME;
            if (TInstant::Now() - *StartTime >= maxTime) {
                return Nothing();
            }
            TDuration delay = (retryClass == ERetryErrorClass::LongRetry) ? Max(CurrentDelay, TDuration::MilliSeconds(200)) : CurrentDelay;
            CurrentDelay = Min(CurrentDelay * 2.0, TDuration::Seconds(30));
            return delay;
        }

        const std::unordered_set<CURLcode> RetriedCurlCodes;
        std::optional<TInstant> StartTime;
        TDuration CurrentDelay = TDuration::MilliSeconds(10);
    };

    struct TFqRetryPolicy : IHTTPGateway::TRetryPolicy {
        explicit TFqRetryPolicy(std::unordered_set<CURLcode> codes) : RetriedCurlCodes(std::move(codes)) {}
        IRetryState::TPtr CreateRetryState() const override {
            return std::make_unique<TFqRetryState>(RetriedCurlCodes);
        }
        std::unordered_set<CURLcode> RetriedCurlCodes;
    };

    return std::make_shared<TFqRetryPolicy>(FqRetriedCurlCodes());
}

}

