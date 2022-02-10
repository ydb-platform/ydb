#include "sliding_window.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSlidingWindow;

Y_UNIT_TEST_SUITE(TSlidingWindowTest) {
    Y_UNIT_TEST(TestSlidingWindowMax) {
        TSlidingWindow<TMaxOperation<unsigned>> w(TDuration::Minutes(5), 5);
        TInstant start = TInstant::MicroSeconds(TDuration::Hours(1).MicroSeconds());
        TInstant now = start;
        w.Update(5, start);                        // ~ ~ ~ ~ 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //         ^
        now += TDuration::Minutes(1) + TDuration::Seconds(1);
        w.Update(5, now);                          // 5 ~ ~ ~ 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); // ^
        now += TDuration::Minutes(1);
        w.Update(3, now);                          // 5 3 ~ ~ 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //   ^
        now += TDuration::Minutes(3);
        w.Update(2, now);                          // 5 3 ~ ~ 2
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //         ^
        now += TDuration::Minutes(1);
        w.Update(2, now);                          // 2 3 ~ ~ 2
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 3); // ^
        now += TDuration::Minutes(1);
        w.Update(2, now);                          // 2 2 ~ ~ 2
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 2); //   ^
        now += TDuration::Minutes(5);
        w.Update(1, now);                          // ~ 1 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 1); //   ^

        // update current bucket
        w.Update(2, now);                          // ~ 2 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 2); //   ^

        w.Update(1, now + TDuration::Seconds(30)); // ~ 2 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 2); //   ^

        // test idle
        now += TDuration::Minutes(1);
        w.Update(now);                             // ~ 2 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 2); //     ^

        now += TDuration::Minutes(5); // ~ ~ ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.Update(now), 0);
    }

    Y_UNIT_TEST(TestSlidingWindowMin) {
        TSlidingWindow<TMinOperation<unsigned>> w(TDuration::Minutes(5), 5);
        TInstant start = TInstant::MicroSeconds(TDuration::Hours(1).MicroSeconds());
        TInstant now = start;
        w.Update(5, start);                        // ~ ~ ~ ~ 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //         ^
        now += TDuration::Minutes(1) + TDuration::Seconds(1);
        w.Update(5, now);                          // 5 ~ ~ ~ 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); // ^
        now += TDuration::Minutes(1);
        w.Update(7, now);                          // 5 7 ~ ~ 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //   ^
        now += TDuration::Minutes(3);
        w.Update(8, now);                          // 5 7 ~ ~ 8
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //         ^
        now += TDuration::Minutes(1);
        w.Update(8, now);                          // 8 7 ~ ~ 8
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 7); // ^
        now += TDuration::Minutes(1);
        w.Update(8, now);                          // 8 8 ~ ~ 8
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 8); //   ^
        now += TDuration::Minutes(5);
        w.Update(6, now);                          // ~ 6 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 6); //   ^

        // update current bucket
        w.Update(5, now);                          // ~ 5 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //   ^

        w.Update(6, now + TDuration::Seconds(30)); // ~ 5 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //   ^

        // test idle
        now += TDuration::Minutes(1);
        w.Update(now);                             // ~ 5 ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //     ^

        now += TDuration::Minutes(5); // ~ ~ ~ ~ ~
        UNIT_ASSERT_VALUES_EQUAL(w.Update(now), std::numeric_limits<unsigned>::max());
    }

    Y_UNIT_TEST(TestSlidingWindowSum) {
        TSlidingWindow<TSumOperation<unsigned>> w(TDuration::Minutes(5), 5);
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 0); // current sum

        TInstant start = TInstant::MicroSeconds(TDuration::Hours(1).MicroSeconds());
        TInstant now = start;
        w.Update(5, start);                        // 0 0 0 0 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 5); //         ^
        now += TDuration::Minutes(1) + TDuration::Seconds(1);
        w.Update(5, now);                           // 5 0 0 0 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 10); // ^
        now += TDuration::Minutes(1);
        w.Update(3, now);                           // 5 3 0 0 5
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 13); //   ^
        now += TDuration::Minutes(3);
        w.Update(2, now);                           // 5 3 0 0 2
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 10); //         ^
        now += TDuration::Minutes(1);
        w.Update(2, now);                          // 2 3 0 0 2
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 7); // ^
        now += TDuration::Minutes(1);
        w.Update(2, now);                          // 2 2 0 0 2
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 6); //   ^
        now += TDuration::Minutes(5);
        w.Update(1, now);                          // 0 1 0 0 0
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 1); //   ^

        // update current bucket
        w.Update(2, now);                          // 0 3 0 0 0
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 3); //   ^

        w.Update(1, now + TDuration::Seconds(30)); // 0 4 0 0 0
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 4); //   ^

        // test idle
        now += TDuration::Minutes(1);
        w.Update(now);                             // 0 4 0 0 0
        UNIT_ASSERT_VALUES_EQUAL(w.GetValue(), 4); //     ^

        now += TDuration::Minutes(5); // 0 0 0 0 0
        UNIT_ASSERT_VALUES_EQUAL(w.Update(now), 0);
    }
}
