#include "throttle.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

namespace {
    void TestZeroSleep(ui64 quota, ui64 step) {
        const TInstant start = Now();
        TThrottle t(TThrottle::TOptions::FromMaxPerInterval(quota, TDuration::Seconds(10)));
        for (ui64 used = 0; (used + step) <= quota; used += step) {
            // No sleeping, so the result should always be complete
            UNIT_ASSERT_EQUAL(step, t.GetQuota(step));
        }
        const TInstant finish = Now();

        // No sleep, therefore execution should be very fast.
        // Checking for 5 seconds to avoid test flaps on loaded systems.
        // If there's a bug and TThrottle sleeps, it will likely be 10 seconds.
        UNIT_ASSERT((finish - start) < TDuration::Seconds(5));
    }

    void TestSleepCount(ui64 quota, ui64 step, ui64 count) {
        const TInstant start = Now();
        TThrottle t(TThrottle::TOptions::FromMaxPerInterval(quota, TDuration::MilliSeconds(10)));
        for (ui64 used = 0; used <= quota * count; ) {
            ui64 avail = t.GetQuota(step);
            UNIT_ASSERT(avail > 0);
            UNIT_ASSERT(avail <= step);
            used += avail;
        }
        const TInstant finish = Now();

        UNIT_ASSERT((finish - start) >= TDuration::MilliSeconds(10 * count));
    }
}

#define TEST_ZERO_SLEEP(quota, step) Y_UNIT_TEST(ZeroSleep_ ## quota ## _ ## step) { TestZeroSleep(quota, step); }
#define TEST_SLEEP(quota, step, count) Y_UNIT_TEST(SleepCount_ ## quota ## _ ## step ## _ ## count) { TestSleepCount(quota, step, count); }

Y_UNIT_TEST_SUITE(ThrottleSuite) {
    TEST_ZERO_SLEEP(1,1)
    TEST_ZERO_SLEEP(10,1)
    TEST_ZERO_SLEEP(1000,1)
    TEST_ZERO_SLEEP(1000,2)
    TEST_ZERO_SLEEP(1000,10)
    TEST_ZERO_SLEEP(1000,17)
    TEST_ZERO_SLEEP(1000,999)

    TEST_SLEEP(1,1,1)
    TEST_SLEEP(10,1,1)
    TEST_SLEEP(1000,1,1)
    TEST_SLEEP(1000,2,1)
    TEST_SLEEP(1000,10,1)
    TEST_SLEEP(1000,17,1)
    TEST_SLEEP(1000,999,1)

    TEST_SLEEP(1,1,2)
    TEST_SLEEP(10,1,2)
    TEST_SLEEP(1000,1,2)
    TEST_SLEEP(1000,2,2)
    TEST_SLEEP(1000,10,2)
    TEST_SLEEP(1000,17,2)
    TEST_SLEEP(1000,999,2)

    TEST_SLEEP(1,1,3)
    TEST_SLEEP(10,1,3)
    TEST_SLEEP(1000,1,3)
    TEST_SLEEP(1000,2,3)
    TEST_SLEEP(1000,10,3)
    TEST_SLEEP(1000,17,3)
    TEST_SLEEP(1000,999,3)

    TEST_SLEEP(1000,50,20)
    TEST_SLEEP(1000,51,20)
}
