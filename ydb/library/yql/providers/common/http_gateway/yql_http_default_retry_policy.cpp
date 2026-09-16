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

// Wraps a standard policy to cut DNS resolution retries short: a host that does not resolve
// is unlikely to start resolving within the full retry budget. The dns budget is counted from
// the first dns error, so a dns error late in the session still gets its own retries. Other
// errors keep the shared backoff and the full budget of the wrapped policy.
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
            if (curlCode != CURLE_COULDNT_RESOLVE_HOST) {
                // The host has been resolved, so a later dns error starts a new budget
                DnsErrorsStartTime.reset();
            } else {
                const TInstant now = TInstant::Now();
                if (!DnsErrorsStartTime) {
                    DnsErrorsStartTime = now;
                } else if (now - *DnsErrorsStartTime >= DNS_ERROR_MAX_TIME) {
                    return Nothing();
                }
            }
            return State->GetNextRetryDelay(curlCode, httpCode);
        }

        const IRetryState::TPtr State;
        std::optional<TInstant> DnsErrorsStartTime; // start of the current run of dns errors
    };

    const IHTTPGateway::TRetryPolicy::TPtr Policy;
};

IHTTPGateway::TRetryPolicy::TPtr GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions&& options) {
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
    return GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{
        .MaxTime = maxTime ? std::make_optional(maxTime) : std::nullopt,
        .MaxRetries = maxRetries,
    });
}

IHTTPGateway::TRetryPolicy::TPtr GetFqHTTPRetryPolicy() {
    return std::make_shared<TFqRetryPolicy>(GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{
        .RetriedCurlCodes = FqRetriedCurlCodes(),
    }));
}

}
