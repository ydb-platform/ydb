#include "meter.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

struct TMockClock {
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<TMockClock, duration>;

    static time_point now() noexcept {
        static int index = 0;
        return index++ < 2 ? time_point() : time_point(std::chrono::seconds(10));
    }
};

using TMockMeter = TMeterImpl<TMockClock>;

Y_UNIT_TEST_SUITE(TMeterTest) { 
    Y_UNIT_TEST(StartsOutWithNoRatesOrCount) { 
        TMeter meter;
        UNIT_ASSERT_EQUAL(meter.GetCount(), 0L);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetMeanRate(), 0.0, 0.0001);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetOneMinuteRate(), 0.0, 0.0001);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetFiveMinutesRate(), 0.0, 0.0001);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetFifteenMinutesRate(), 0.0, 0.0001);
    }

    Y_UNIT_TEST(MarksEventsAndUpdatesRatesAndCount) { 
        TMockMeter meter;
        meter.Mark();
        meter.Mark(2);
        UNIT_ASSERT_EQUAL(meter.GetCount(), 3L);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetMeanRate(), 0.3, 0.001);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetOneMinuteRate(), 0.1840, 0.0001);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetFiveMinutesRate(), 0.1966, 0.0001);
        UNIT_ASSERT_DOUBLES_EQUAL(meter.GetFifteenMinutesRate(), 0.1988, 0.0001);
    }
}
