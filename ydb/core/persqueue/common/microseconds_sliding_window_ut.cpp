#include "microseconds_sliding_window.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TMicrosecondsSlidingWindow) {

Y_UNIT_TEST(Basic) {
    TMicrosecondsSlidingWindow sw(60, TDuration::Minutes(1));
    TInstant now = TInstant::Now();

    sw.Update(TDuration::Seconds(5).MicroSeconds(), now);
    now += TDuration::Seconds(58);
    sw.Update(now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 2'000'000);

    now += TDuration::Seconds(2);
    sw.Update(now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 0);

    sw.Update(TDuration::Seconds(5).MicroSeconds(), now);
    now += TDuration::Seconds(10);
    sw.Update(TDuration::Seconds(5).MicroSeconds(), now);
    now += TDuration::Seconds(10);
    sw.Update(TDuration::Seconds(5).MicroSeconds(), now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 15'000'000);

    now += TDuration::Seconds(36);
    sw.Update(now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 14'000'000);

    now += TDuration::Seconds(20);
    sw.Update(now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 4'000'000);

    now += TDuration::Seconds(4);
    sw.Update(now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 0);

    now += TDuration::Seconds(50);
    sw.Update(TDuration::Seconds(60).MicroSeconds(), now);
    now += TDuration::Seconds(20);
    sw.Update(now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 40'000'000);

    now += TDuration::Seconds(180);
    sw.Update(TDuration::Seconds(180).MicroSeconds(), now);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 60'000'000);
}

Y_UNIT_TEST(CapsValueAndIgnoresInWindowAdvances) {
    TMicrosecondsSlidingWindow sw(4, TDuration::Seconds(4));
    const TInstant t0 = TInstant::MicroSeconds(1'000'000'000);

    UNIT_ASSERT_EQUAL(sw.Update(t0), 0);
    UNIT_ASSERT_EQUAL(sw.Update(10'000'000, t0), 4'000'000);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 4'000'000);

    UNIT_ASSERT_EQUAL(sw.Update(t0 + TDuration::MilliSeconds(500)), 4'000'000);
}

Y_UNIT_TEST(IgnoresTimestampInsideCurrentWindow) {
    TMicrosecondsSlidingWindow sw(4, TDuration::Seconds(4));
    const TInstant t0 = TInstant::MicroSeconds(5'000'000'000);

    UNIT_ASSERT_EQUAL(sw.Update(1'000'000, t0), 1'000'000);
    UNIT_ASSERT_EQUAL(sw.Update(t0 - TDuration::MicroSeconds(1)), 1'000'000);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 1'000'000);
}

Y_UNIT_TEST(ExpiresWholeWindow) {
    TMicrosecondsSlidingWindow sw(4, TDuration::Seconds(4));
    const TInstant t0 = TInstant::MicroSeconds(2'000'000'000);
    sw.Update(1'000'000, t0);
    UNIT_ASSERT(sw.GetValue() > 0);
    UNIT_ASSERT_EQUAL(sw.Update(t0 + TDuration::Seconds(10)), 0);
    UNIT_ASSERT_EQUAL(sw.GetValue(), 0);
}

Y_UNIT_TEST(AdvancesAcrossBucketsAndWraps) {
    TMicrosecondsSlidingWindow sw(4, TDuration::Seconds(4));
    const TInstant t0 = TInstant::MicroSeconds(3'000'000'000);

    sw.Update(4'000'000, t0);
    sw.Update(1'000'000, t0 + TDuration::Seconds(1));
    sw.Update(t0 + TDuration::Seconds(3));
    UNIT_ASSERT(sw.GetValue() <= 4'000'000);

    sw.Update(500'000, t0 + TDuration::Seconds(4));
    sw.Update(t0 + TDuration::Seconds(5));
    UNIT_ASSERT(sw.GetValue() <= 4'000'000);
}

Y_UNIT_TEST(ClearsFirstNonZeroThenFindsWrappedBucket) {
    TMicrosecondsSlidingWindow sw(4, TDuration::Seconds(4));
    const TInstant t0 = TInstant::MicroSeconds(4'000'000'000);

    sw.Update(3'000'000, t0);
    sw.Update(2'000'000, t0 + TDuration::Seconds(1));
    sw.Update(t0 + TDuration::Seconds(2));
    sw.Update(t0 + TDuration::Seconds(3));
    sw.Update(t0 + TDuration::Seconds(4));
    UNIT_ASSERT(sw.GetValue() <= 3'000'000);
}

} //Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPQ
