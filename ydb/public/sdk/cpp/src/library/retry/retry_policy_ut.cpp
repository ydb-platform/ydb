#include "retry_policy.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(RetryPolicy) {
    Y_UNIT_TEST(NoRetryPolicy) {
        auto policy = IRetryPolicy<int>::GetNoRetryPolicy();
        UNIT_ASSERT(!policy->CreateRetryState()->GetNextRetryDelay(42));
    }

    using ITestPolicy = IRetryPolicy<ERetryErrorClass>;

    ERetryErrorClass ErrorClassFunction(ERetryErrorClass err) {
        return err;
    }

#define ASSERT_INTERVAL(from, to, val) {        \
        auto v = val;                           \
        UNIT_ASSERT(v);                         \
        UNIT_ASSERT_GE_C(*v, from, *v);         \
        UNIT_ASSERT_LE_C(*v, to, *v);           \
    }

    Y_UNIT_TEST(FixedIntervalPolicy) {
        auto policy = ITestPolicy::GetFixedIntervalPolicy(ErrorClassFunction, TDuration::MilliSeconds(100), TDuration::Seconds(100));
        auto state = policy->CreateRetryState();
        for (int i = 0; i < 5; ++i) {
            ASSERT_INTERVAL(TDuration::MilliSeconds(50), TDuration::MilliSeconds(100), state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
            ASSERT_INTERVAL(TDuration::Seconds(50), TDuration::Seconds(100), state->GetNextRetryDelay(ERetryErrorClass::LongRetry));
            UNIT_ASSERT(!state->GetNextRetryDelay(ERetryErrorClass::NoRetry));
        }
    }

    Y_UNIT_TEST(ExponentialBackoffPolicy) {
        auto policy = ITestPolicy::GetExponentialBackoffPolicy(ErrorClassFunction, TDuration::MilliSeconds(100), TDuration::Seconds(100), TDuration::Seconds(500));
        auto state = policy->CreateRetryState();

        // Step 1
        ASSERT_INTERVAL(TDuration::MilliSeconds(50), TDuration::MilliSeconds(100), state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));

        // Step 2
        ASSERT_INTERVAL(TDuration::Seconds(50), TDuration::Seconds(100), state->GetNextRetryDelay(ERetryErrorClass::LongRetry));

        // Step 3
        ASSERT_INTERVAL(TDuration::Seconds(100), TDuration::Seconds(200), state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));

        // Step 4
        ASSERT_INTERVAL(TDuration::Seconds(200), TDuration::Seconds(400), state->GetNextRetryDelay(ERetryErrorClass::LongRetry));

        // Step 5. Max delay
        ASSERT_INTERVAL(TDuration::Seconds(250), TDuration::Seconds(500), state->GetNextRetryDelay(ERetryErrorClass::LongRetry));
        ASSERT_INTERVAL(TDuration::Seconds(250), TDuration::Seconds(500), state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));

        // No retry
        UNIT_ASSERT(!state->GetNextRetryDelay(ERetryErrorClass::NoRetry));
    }

    void TestMaxRetries(bool exponentialBackoff) {
        ITestPolicy::TPtr policy;
        if (exponentialBackoff) {
            policy = ITestPolicy::GetExponentialBackoffPolicy(ErrorClassFunction, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(30), 3);
        } else {
            policy = ITestPolicy::GetFixedIntervalPolicy(ErrorClassFunction, TDuration::MilliSeconds(100), TDuration::MilliSeconds(300), 3);
        }
        auto state = policy->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
        UNIT_ASSERT(state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
        UNIT_ASSERT(state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
        UNIT_ASSERT(!state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
        UNIT_ASSERT(!state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
    }

    void TestMaxTime(bool exponentialBackoff) {
        ITestPolicy::TPtr policy;
        const TDuration maxDelay = TDuration::Seconds(2);
        if (exponentialBackoff) {
            policy = ITestPolicy::GetExponentialBackoffPolicy(ErrorClassFunction, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(30), 100500, maxDelay);
        } else {
            policy = ITestPolicy::GetFixedIntervalPolicy(ErrorClassFunction, TDuration::MilliSeconds(100), TDuration::MilliSeconds(300), 100500, maxDelay);
        }
        const TInstant start = TInstant::Now();
        auto state = policy->CreateRetryState();
        for (int i = 0; i < 3; ++i) {
            auto delay = state->GetNextRetryDelay(ERetryErrorClass::ShortRetry);
            const TInstant now = TInstant::Now();
            UNIT_ASSERT(delay || now - start >= maxDelay);
        }
        Sleep(maxDelay);
        UNIT_ASSERT(!state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
        UNIT_ASSERT(!state->GetNextRetryDelay(ERetryErrorClass::ShortRetry));
    }

    Y_UNIT_TEST(MaxRetriesExponentialBackoff) {
        TestMaxRetries(true);
    }

    Y_UNIT_TEST(MaxRetriesFixedInterval) {
        TestMaxRetries(false);
    }

    Y_UNIT_TEST(MaxTimeExponentialBackoff) {
        TestMaxTime(true);
    }

    Y_UNIT_TEST(MaxTimeFixedInterval) {
        TestMaxTime(false);
    }
}
