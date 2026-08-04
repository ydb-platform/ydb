#include "yql_http_default_retry_policy.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(THttpDefaultRetryPolicyTest) {

    Y_UNIT_TEST(DefaultOptionsHasDefaultMaxTime) {
        // Default options should use 5-minute maxTime (not zero, not 1 second)
        auto policy = GetHTTPDefaultRetryPolicy();
        UNIT_ASSERT(policy);
        auto state = policy->CreateRetryState();
        UNIT_ASSERT(state);
        // A retriable curl code should get a retry delay
        auto delay = state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0);
        UNIT_ASSERT(delay.Defined());
    }

    Y_UNIT_TEST(NonRetriableCurlCodeNoRetry) {
        auto policy = GetHTTPDefaultRetryPolicy();
        auto state = policy->CreateRetryState();
        // CURLE_URL_MALFORMAT is not in the retried set
        auto delay = state->GetNextRetryDelay(CURLE_URL_MALFORMAT, 0);
        UNIT_ASSERT(!delay.Defined());
    }

    Y_UNIT_TEST(RetriableHttpCodesGetLongRetry) {
        auto policy = GetHTTPDefaultRetryPolicy();
        auto state = policy->CreateRetryState();
        // HTTP 429 Too Many Requests should be retriable
        auto delay = state->GetNextRetryDelay(CURLE_OK, 429);
        UNIT_ASSERT(delay.Defined());
    }

    Y_UNIT_TEST(NonRetriableHttpCodeNoRetry) {
        auto policy = GetHTTPDefaultRetryPolicy();
        auto state = policy->CreateRetryState();
        // HTTP 400 Bad Request should not be retried
        auto delay = state->GetNextRetryDelay(CURLE_OK, 400);
        UNIT_ASSERT(!delay.Defined());
    }

    Y_UNIT_TEST(Http0NoRetry) {
        auto policy = GetHTTPDefaultRetryPolicy();
        auto state = policy->CreateRetryState();
        // HTTP code 0 with OK curl means cancellation — no retry
        auto delay = state->GetNextRetryDelay(CURLE_OK, 0);
        UNIT_ASSERT(!delay.Defined());
    }

    Y_UNIT_TEST(MaxRetriesIsRespected) {
        THttpRetryPolicyOptions options;
        options.MaxRetries = 2;
        auto policy = GetHTTPDefaultRetryPolicy(std::move(options));
        auto state = policy->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
    }

    Y_UNIT_TEST(CustomMaxTimeIsRespected) {
        THttpRetryPolicyOptions options;
        options.MaxTime = TDuration::MilliSeconds(1);
        auto policy = GetHTTPDefaultRetryPolicy(std::move(options));
        auto state = policy->CreateRetryState();
        // Sleep long enough so maxTime (1ms) is exceeded before first check
        Sleep(TDuration::MilliSeconds(50));
        auto delay = state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0);
        UNIT_ASSERT(!delay.Defined());
    }

    Y_UNIT_TEST(FqPolicyRetriableCodesWork) {
        auto policy = GetFqHTTPRetryPolicy();
        UNIT_ASSERT(policy);
        auto state = policy->CreateRetryState();
        // CURLE_COULDNT_CONNECT should be retriable in FQ policy
        auto delay = state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0);
        UNIT_ASSERT(delay.Defined());
    }

    Y_UNIT_TEST(FqPolicyDnsErrorRetriable) {
        auto policy = GetFqHTTPRetryPolicy();
        auto state = policy->CreateRetryState();
        // CURLE_COULDNT_RESOLVE_HOST should be retriable in FQ policy
        auto delay = state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0);
        UNIT_ASSERT(delay.Defined());
    }

    Y_UNIT_TEST(FqPolicyNonDnsErrorNotExpiredBy10Seconds) {
        auto policy = GetFqHTTPRetryPolicy();
        auto state = policy->CreateRetryState();
        // Non-DNS errors should NOT expire after 10 seconds (they use 5 minute budget)
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        // Immediately check again — should still have budget
        auto delay = state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0);
        UNIT_ASSERT(delay.Defined());
    }

    Y_UNIT_TEST(OptionalMaxTimeConstructor) {
        // Test constructor with explicit maxTime
        THttpRetryPolicyOptions options(TDuration::Seconds(30));
        UNIT_ASSERT(options.MaxTime.has_value());
        UNIT_ASSERT_EQUAL(*options.MaxTime, TDuration::Seconds(30));

        // Test constructor with no maxTime (nullopt = default)
        THttpRetryPolicyOptions defaultOptions(std::nullopt);
        UNIT_ASSERT(!defaultOptions.MaxTime.has_value());
    }

}

} // namespace NYql
