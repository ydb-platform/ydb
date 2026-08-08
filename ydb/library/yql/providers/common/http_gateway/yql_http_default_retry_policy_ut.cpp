#include "yql_http_default_retry_policy.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(THttpDefaultRetryPolicyTest) {

    Y_UNIT_TEST(RetriableCurlCode) {
        auto state = GetHTTPDefaultRetryPolicy()->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
    }

    Y_UNIT_TEST(NonRetriableCurlCode) {
        auto state = GetHTTPDefaultRetryPolicy()->CreateRetryState();
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_URL_MALFORMAT, 0).Defined());
    }

    Y_UNIT_TEST(RetriableHttpCodes) {
        for (long httpCode : {408, 425, 429, 500, 502, 503, 504}) {
            auto state = GetHTTPDefaultRetryPolicy()->CreateRetryState();
            UNIT_ASSERT_C(state->GetNextRetryDelay(CURLE_OK, httpCode).Defined(), httpCode);
        }
    }

    Y_UNIT_TEST(NonRetriableHttpCodes) {
        // 0 is the rare case when curl code is not available, e.g. manual cancelling
        for (long httpCode : {0, 200, 400, 403, 404, 501}) {
            auto state = GetHTTPDefaultRetryPolicy()->CreateRetryState();
            UNIT_ASSERT_C(!state->GetNextRetryDelay(CURLE_OK, httpCode).Defined(), httpCode);
        }
    }

    Y_UNIT_TEST(RetriesInternalServerError) {
        auto state = GetHTTPDefaultRetryPolicy()->CreateRetryState();
        TDuration prevDelay;
        for (size_t i = 0; i < 5; ++i) {
            auto delay = state->GetNextRetryDelay(CURLE_OK, 500);
            UNIT_ASSERT_C(delay.Defined(), i);
            // 500 is a long retry, so the delay starts from minLongRetryDelay (200ms) and grows.
            // RandomizeDelay returns half of the current delay plus a random part of the other half.
            UNIT_ASSERT_GE_C(*delay, TDuration::MilliSeconds(100), i);
            UNIT_ASSERT_LE_C(prevDelay, *delay, i);
            prevDelay = *delay;
        }
    }

    Y_UNIT_TEST(FqPolicyRetriesInternalServerError) {
        auto state = GetFqHTTPRetryPolicy()->CreateRetryState();
        for (size_t i = 0; i < 5; ++i) {
            auto delay = state->GetNextRetryDelay(CURLE_OK, 500);
            UNIT_ASSERT_C(delay.Defined(), i);
            UNIT_ASSERT_GE_C(*delay, TDuration::MilliSeconds(100), i);
        }
    }

    Y_UNIT_TEST(MaxRetries) {
        auto state = GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{.MaxRetries = 2})->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
    }

    Y_UNIT_TEST(MaxTime) {
        // maxTime has to stay above minDelay (10ms), see Y_ASSERT in TExponentialBackoffPolicy
        auto state = GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{.MaxTime = TDuration::MilliSeconds(50)})->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        Sleep(TDuration::MilliSeconds(150));
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
    }

    Y_UNIT_TEST(ZeroMaxTimeMeansDefaultMaxTime) {
        // Zero has to be translated to the default maxTime, not passed through as an expired budget
        auto state = GetHTTPDefaultRetryPolicy(TDuration::Zero())->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
    }

    Y_UNIT_TEST(DefaultOptionsUseYqlRetriedCurlCodes) {
        UNIT_ASSERT(THttpRetryPolicyOptions{}.RetriedCurlCodes == YqlRetriedCurlCodes());
    }

    Y_UNIT_TEST(CustomRetriedCurlCodes) {
        auto state = GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{.RetriedCurlCodes = {CURLE_URL_MALFORMAT}})->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_URL_MALFORMAT, 0).Defined());
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
    }

    Y_UNIT_TEST(FqPolicyUsesFqRetriedCurlCodes) {
        // These codes are retried by the Fq policy only
        for (CURLcode curlCode : {CURLE_PARTIAL_FILE, CURLE_GOT_NOTHING, CURLE_COULDNT_RESOLVE_HOST}) {
            auto fqState = GetFqHTTPRetryPolicy()->CreateRetryState();
            UNIT_ASSERT_C(fqState->GetNextRetryDelay(curlCode, 0).Defined(), int(curlCode));

            auto defaultState = GetHTTPDefaultRetryPolicy()->CreateRetryState();
            UNIT_ASSERT_C(!defaultState->GetNextRetryDelay(curlCode, 0).Defined(), int(curlCode));
        }
    }

    Y_UNIT_TEST(FqPolicyRetriableHttpCodes) {
        auto state = GetFqHTTPRetryPolicy()->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_OK, 503).Defined());
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_OK, 404).Defined());
    }

    Y_UNIT_TEST(FqPolicySharesRetryBudgetBetweenDnsAndOtherErrors) {
        // Dns errors are not retried on their own schedule, they consume the shared budget
        auto state = GetFqHTTPRetryPolicy()->CreateRetryState();
        for (size_t i = 0; i < 10; ++i) {
            UNIT_ASSERT_C(state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0).Defined(), i);
        }
        // Backoff has grown past minDelay for the non dns error as well
        UNIT_ASSERT_GT(*state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0), TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(FqPolicyDnsErrorsAreGivenUpEarly) {
        auto state = GetFqHTTPRetryPolicy()->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0).Defined());

        // Dns errors get 10 seconds, everything else keeps the default 5 minutes
        Sleep(TDuration::Seconds(11));
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0).Defined());
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
    }

    Y_UNIT_TEST(FqPolicyDnsErrorBudgetRestartsAfterOtherError) {
        auto state = GetFqHTTPRetryPolicy()->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0).Defined());
        Sleep(TDuration::Seconds(11));

        // The host has been resolved in between, so the dns budget starts over and the dns
        // error is retried even though the first dns error is more than 10 seconds old
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0).Defined());
    }

    Y_UNIT_TEST(FqPolicyDnsErrorAfterOtherErrorsIsRetried) {
        auto state = GetFqHTTPRetryPolicy()->CreateRetryState();
        // Other errors use up more than the dns budget before the first dns error arrives
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());
        Sleep(TDuration::Seconds(11));
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_CONNECT, 0).Defined());

        // The dns budget is counted from the first dns error, not from the start of the session
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0).Defined());
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_COULDNT_RESOLVE_HOST, 0).Defined());
    }

}

} // namespace NYql
