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
// TFqRetryState delegates to two standard exponential-backoff states, selecting by curl code.
IHTTPGateway::TRetryPolicy::TPtr GetFqHTTPRetryPolicy(THttpRetryPolicyOptions&& options) {
    auto dnsMaxTime = options.DnsMaxTime.value_or(DNS_ERROR_MAX_TIME);
    auto maxTime = options.MaxTime.value_or(DEFAULT_MAX_TIME);

    auto makeDnsPolicy = [dnsMaxTime]() {
        return IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy(
            [](CURLcode curlCode, long httpCode) {
                return curlCode == CURLE_COULDNT_RESOLVE_HOST ? ERetryErrorClass::ShortRetry : ERetryErrorClass::NoRetry;
            },
            TDuration::MilliSeconds(10),  // minDelay
            TDuration::MilliSeconds(200), // minLongRetryDelay
            TDuration::Seconds(30),       // maxDelay
            std::numeric_limits<size_t>::max(),
            dnsMaxTime);
    };

    auto makeOtherPolicy = [maxTime](std::unordered_set<CURLcode> codes) {
        return IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy(
            [codes = std::move(codes)](CURLcode curlCode, long httpCode) {
                return ClassifyError(curlCode, httpCode, codes);
            },
            TDuration::MilliSeconds(10),  // minDelay
            TDuration::MilliSeconds(200), // minLongRetryDelay
            TDuration::Seconds(30),       // maxDelay
            std::numeric_limits<size_t>::max(),
            maxTime);
    };

    struct TFqRetryState : IHTTPGateway::TRetryPolicy::IRetryState {
        TFqRetryState(IHTTPGateway::TRetryPolicy::TPtr dnsPolicy, IHTTPGateway::TRetryPolicy::TPtr otherPolicy)
            : DnsState(dnsPolicy->CreateRetryState())
            , OtherState(otherPolicy->CreateRetryState())
        {}

        TMaybe<TDuration> GetNextRetryDelay(CURLcode curlCode, long httpCode) override {
            return curlCode == CURLE_COULDNT_RESOLVE_HOST
                ? DnsState->GetNextRetryDelay(curlCode, httpCode)
                : OtherState->GetNextRetryDelay(curlCode, httpCode);
        }

        IHTTPGateway::TRetryPolicy::IRetryState::TPtr DnsState;
        IHTTPGateway::TRetryPolicy::IRetryState::TPtr OtherState;
    };

    struct TFqRetryPolicy : IHTTPGateway::TRetryPolicy {
        TFqRetryPolicy(IHTTPGateway::TRetryPolicy::TPtr dnsPolicy, IHTTPGateway::TRetryPolicy::TPtr otherPolicy)
            : DnsPolicy(std::move(dnsPolicy))
            , OtherPolicy(std::move(otherPolicy))
        {}
        IRetryState::TPtr CreateRetryState() const override {
            return std::make_unique<TFqRetryState>(DnsPolicy, OtherPolicy);
        }
        IHTTPGateway::TRetryPolicy::TPtr DnsPolicy;
        IHTTPGateway::TRetryPolicy::TPtr OtherPolicy;
    };

    auto fqCodes = options.RetriedCurlCodes.empty() ? FqRetriedCurlCodes() : std::move(options.RetriedCurlCodes);
    return std::make_shared<TFqRetryPolicy>(makeDnsPolicy(), makeOtherPolicy(std::move(fqCodes)));
}

}

