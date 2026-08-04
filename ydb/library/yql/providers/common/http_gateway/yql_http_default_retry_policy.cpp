#include "yql_http_default_retry_policy.h"

namespace NYql {

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

ERetryErrorClass ClassifyError(CURLcode curlCode, long httpCode, const std::unordered_set<CURLcode>& retriedCurlCodes) {
    if (curlCode != CURLE_OK) {
        return retriedCurlCodes.contains(curlCode) ? ERetryErrorClass::ShortRetry : ERetryErrorClass::NoRetry;
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
}

IHTTPGateway::TRetryPolicy::TPtr MakeRetryPolicy(std::unordered_set<CURLcode> retriedCurlCodes, TDuration maxTime, size_t maxRetries) {
    return IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy(
        [retriedCurlCodes = std::move(retriedCurlCodes)](CURLcode curlCode, long httpCode) {
            return ClassifyError(curlCode, httpCode, retriedCurlCodes);
        },
        TDuration::MilliSeconds(10), // minDelay
        TDuration::MilliSeconds(200), // minLongRetryDelay
        TDuration::Seconds(30), // maxDelay
        maxRetries, // maxRetries
        maxTime); // maxTime
}

// Wraps a standard policy to cut DNS resolution retries short: a host that does not resolve
// is unlikely to start resolving within the full retry budget. Other errors keep the shared
// backoff and the full budget of the wrapped policy.
class TFqRetryPolicy final: public IHTTPGateway::TRetryPolicy {
public:
    explicit TFqRetryPolicy(IHTTPGateway::TRetryPolicy::TPtr policy)
        : Policy(std::move(policy))
    {
    }

    IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TFqRetryState>(Policy->CreateRetryState());
    }

private:
    struct TFqRetryState final: IRetryState {
        explicit TFqRetryState(IRetryState::TPtr state)
            : State(std::move(state))
        {
        }

        TMaybe<TDuration> GetNextRetryDelay(CURLcode curlCode, long httpCode) override {
            if (curlCode == CURLE_COULDNT_RESOLVE_HOST && TInstant::Now() - StartTime >= DNS_ERROR_MAX_TIME) {
                return Nothing();
            }
            return State->GetNextRetryDelay(curlCode, httpCode);
        }

        const IRetryState::TPtr State;
        const TInstant StartTime = TInstant::Now();
    };

    const IHTTPGateway::TRetryPolicy::TPtr Policy;
};

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions&& options) {
    return MakeRetryPolicy(std::move(options.RetriedCurlCodes), options.MaxTime.value_or(DEFAULT_MAX_TIME), options.MaxRetries);
}

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(TDuration maxTime, size_t maxRetries) {
    return GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{
        .MaxTime = maxTime ? std::make_optional(maxTime) : std::nullopt,
        .MaxRetries = maxRetries,
    });
}

IHTTPGateway::TRetryPolicy::TPtr GetFqHTTPRetryPolicy() {
    return std::make_shared<TFqRetryPolicy>(MakeRetryPolicy(FqRetriedCurlCodes(), DEFAULT_MAX_TIME, std::numeric_limits<size_t>::max()));
}

}
