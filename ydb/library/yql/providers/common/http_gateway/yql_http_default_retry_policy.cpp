#include "yql_http_default_retry_policy.h"

namespace NYql {

namespace {

constexpr TDuration DEFAULT_MAX_TIME = TDuration::Minutes(5);
constexpr TDuration DNS_ERROR_MAX_TIME = TDuration::Seconds(10);

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
        CURLE_PARTIAL_FILE,
        CURLE_NO_CONNECTION_AVAILABLE,
        CURLE_GOT_NOTHING,
        CURLE_COULDNT_RESOLVE_HOST
    };
}

// Custom retry policy for FQ that applies a shorter time budget for DNS resolution errors
class TFqRetryPolicy : public IHTTPGateway::TRetryPolicy {
public:
    explicit TFqRetryPolicy(std::unordered_set<CURLcode> retriedCurlCodes)
        : RetriedCurlCodes(std::move(retriedCurlCodes))
    {}

    struct TFqRetryState : IRetryState {
        explicit TFqRetryState(std::unordered_set<CURLcode> retriedCurlCodes)
            : RetriedCurlCodes(std::move(retriedCurlCodes))
        {}

        TMaybe<TDuration> GetNextRetryDelay(CURLcode curlCode, long httpCode) override {
            if (!StartTime) {
                StartTime = TInstant::Now();
            }

            auto elapsed = TInstant::Now() - *StartTime;

            auto retryClass = GetRetryClass(curlCode, httpCode);
            if (retryClass == ERetryErrorClass::NoRetry) {
                return Nothing();
            }

            // DNS errors get a much shorter retry window
            TDuration effectiveMaxTime = (curlCode == CURLE_COULDNT_RESOLVE_HOST)
                ? DNS_ERROR_MAX_TIME
                : DEFAULT_MAX_TIME;

            if (elapsed >= effectiveMaxTime) {
                return Nothing();
            }

            TDuration delay = CurrentDelay;
            if (retryClass == ERetryErrorClass::LongRetry) {
                delay = Max(delay, TDuration::MilliSeconds(200));
            }
            CurrentDelay = Min(CurrentDelay * 2.0, TDuration::Seconds(30));
            return delay;
        }

    private:
        ERetryErrorClass GetRetryClass(CURLcode curlCode, long httpCode) const {
            if (curlCode != CURLE_OK) {
                return RetriedCurlCodes.contains(curlCode)
                    ? ERetryErrorClass::ShortRetry
                    : ERetryErrorClass::NoRetry;
            }

            switch (httpCode) {
                case 0:
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
        }

        const std::unordered_set<CURLcode> RetriedCurlCodes;
        std::optional<TInstant> StartTime;
        TDuration CurrentDelay = TDuration::MilliSeconds(10);
    };

    IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TFqRetryState>(RetriedCurlCodes);
    }

private:
    std::unordered_set<CURLcode> RetriedCurlCodes;
};

} // namespace

THttpRetryPolicyOptions::THttpRetryPolicyOptions()
    : RetriedCurlCodes(YqlRetriedCurlCodes())
{}

THttpRetryPolicyOptions::THttpRetryPolicyOptions(std::optional<TDuration> maxTime, size_t maxRetries)
    : MaxTime(maxTime)
    , MaxRetries(maxRetries)
    , RetriedCurlCodes(YqlRetriedCurlCodes())
{}

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions options) {
    auto maxTime = options.MaxTime.value_or(DEFAULT_MAX_TIME);
    auto maxRetries = options.MaxRetries;
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
    return GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{maxTime ? std::make_optional(maxTime) : std::nullopt, maxRetries});
}

IHTTPGateway::TRetryPolicy::TPtr GetFqHTTPRetryPolicy() {
    return std::make_shared<TFqRetryPolicy>(FqRetriedCurlCodes());
}

}
