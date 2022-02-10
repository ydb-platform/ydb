#include <library/cpp/testing/unittest/registar.h>
#include "token_bucket.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(TTokenBucketTest) {

    static constexpr double Eps = 1e-9;

    Y_UNIT_TEST(Unlimited) {
        TTokenBucket tb;
        UNIT_ASSERT(tb.Available() == std::numeric_limits<double>::infinity());
        tb.Take(100500);
        UNIT_ASSERT(tb.Available() == std::numeric_limits<double>::infinity());
        TInstant now = TInstant::Now();
        tb.Fill(now);
        now += TDuration::Seconds(1);
        UNIT_ASSERT(tb.Available() == std::numeric_limits<double>::infinity());
        UNIT_ASSERT_DOUBLES_EQUAL(tb.FillAndTryTake(now, 42), 42, Eps);
    }

    Y_UNIT_TEST(Limited) {
        TTokenBucket tb;
        tb.SetRate(100);
        tb.SetCapacity(1000);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 1000, Eps);
        TInstant now = TInstant::Now();
        tb.Fill(now);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 1000, Eps);
        tb.Take(500);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 500, Eps);
        now += TDuration::Seconds(1);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.FillAndTryTake(now, 100500), 600, Eps);
        now += TDuration::Days(1);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.FillAndTryTake(now, 100500), 1000, Eps);
        now += TDuration::Seconds(1);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.FillAndTryTake(now, 100500), 100, Eps);
        now += TDuration::Seconds(2);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.FillAndTryTake(now, 100), 100, Eps);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.FillAndTryTake(now, 50), 50, Eps);
        now += TDuration::Seconds(1);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.FillAndTryTake(now, 200), 150, Eps);
        tb.Take(100);
        now += TDuration::Seconds(3);
        tb.Fill(now);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 200, Eps);

        tb.SetRate(200);
        now += TDuration::Seconds(1);
        tb.Fill(now);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 400, Eps);
        now += TDuration::Seconds(1);
        tb.Fill(now);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 600, Eps);
        now += TDuration::Days(365);
        tb.Fill(now);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 1000, Eps);

        tb.SetCapacity(500);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 500, Eps);
        tb.SetCapacity(2000);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 500, Eps);
        now += TDuration::Days(365);
        tb.Fill(now);
        UNIT_ASSERT_DOUBLES_EQUAL(tb.Available(), 2000, Eps);

        tb.SetUnlimited();
        UNIT_ASSERT(tb.Available() == std::numeric_limits<double>::infinity());
    }

    Y_UNIT_TEST(DelayCalculation) {
        TTokenBucket tb;
        TInstant now = TInstant::Now();
        tb.SetRate(150);
        tb.SetCapacity(1);
        tb.Fill(now);

        UNIT_ASSERT_VALUES_EQUAL(tb.NextAvailableDelay(), TDuration::Zero());
        tb.Take(2);

        auto expectedDelay = TDuration::MicroSeconds(std::ceil(1000000.0 / 150));
        UNIT_ASSERT_VALUES_EQUAL(tb.NextAvailableDelay(), expectedDelay);

        now += expectedDelay;
        tb.Fill(now);
        UNIT_ASSERT_VALUES_EQUAL(tb.NextAvailableDelay(), TDuration::Zero());
    }
}

} // NKikimr
