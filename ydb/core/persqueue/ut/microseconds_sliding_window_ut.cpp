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

} //Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPQ
