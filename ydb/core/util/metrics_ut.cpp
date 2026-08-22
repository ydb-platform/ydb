#include "tuples.h"
#include "metrics.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMetrics {

Y_UNIT_TEST_SUITE(TExponentialMovingAverageValueTest) {

    static constexpr double Eps = 1e-9;

    Y_UNIT_TEST(FirstPushSetsValue) {
        TExponentialMovingAverageValue<double> avg(TDuration::Seconds(1));
        UNIT_ASSERT(!avg.IsValueReady());
        avg.Push(100, TInstant::Seconds(1));
        UNIT_ASSERT(avg.IsValueReady());
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 100, Eps);
    }

    Y_UNIT_TEST(HalfLife) {
        TInstant now = TInstant::Seconds(1);
        TExponentialMovingAverageValue<double> avg(TDuration::Seconds(10));
        avg.Push(100, now);
        // after exactly one half-life the old value's weight is 1/2
        now += TDuration::Seconds(10);
        avg.Push(0, now);
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 50, Eps);
        // after another half-life
        now += TDuration::Seconds(10);
        avg.Push(0, now);
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 25, Eps);
    }

    Y_UNIT_TEST(HalfLifeIsIndependentOfPushFrequency) {
        // decaying from 100 to 0 over one half-life must give the same result
        // whether it is done in one push or in many small ones
        TInstant start = TInstant::Seconds(1);
        TExponentialMovingAverageValue<double> coarse(TDuration::Seconds(10));
        coarse.Push(100, start);
        coarse.Push(0, start + TDuration::Seconds(10));

        TExponentialMovingAverageValue<double> fine(TDuration::Seconds(10));
        fine.Push(100, start);
        for (ui64 ms = 100; ms <= 10000; ms += 100) {
            fine.Push(0, start + TDuration::MilliSeconds(ms));
        }
        UNIT_ASSERT_DOUBLES_EQUAL(coarse.GetValue(), fine.GetValue(), Eps);
        UNIT_ASSERT_DOUBLES_EQUAL(fine.GetValue(), 50, Eps);
    }

    Y_UNIT_TEST(ConvergesToConstantInput) {
        TInstant now = TInstant::Seconds(1);
        TExponentialMovingAverageValue<double> avg(TDuration::Seconds(1));
        for (int i = 0; i < 100; ++i) {
            avg.Push(42, now);
            now += TDuration::Seconds(1);
        }
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 42, 1e-6);
    }

    Y_UNIT_TEST(SameTimestampPushIsIgnored) {
        TInstant now = TInstant::Seconds(1);
        TExponentialMovingAverageValue<double> avg(TDuration::Seconds(10));
        avg.Push(100, now);
        avg.Push(0, now);
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 100, Eps);
    }

    Y_UNIT_TEST(TimeGoingBackwardsIsIgnored) {
        TInstant now = TInstant::Seconds(100);
        TExponentialMovingAverageValue<double> avg(TDuration::Seconds(10));
        avg.Push(100, now);
        avg.Push(0, now - TDuration::Seconds(50));
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 100, Eps);
    }

    Y_UNIT_TEST(SetHalfLifeTime) {
        TInstant now = TInstant::Seconds(1);
        TExponentialMovingAverageValue<double> avg(TDuration::Seconds(10));
        avg.Push(100, now);
        // reconfigured half-life applies to subsequent pushes
        avg.SetHalfLifeTime(TDuration::Seconds(20));
        now += TDuration::Seconds(20);
        avg.Push(0, now);
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 50, Eps);
    }

    Y_UNIT_TEST(ZeroHalfLifeMeansNoSmoothing) {
        TInstant now = TInstant::Seconds(1);
        TExponentialMovingAverageValue<double> avg(TDuration::Zero());
        avg.Push(100, now);
        now += TDuration::MilliSeconds(1);
        avg.Push(7, now);
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 7, Eps);
        // switching from a real half-life to zero also stops smoothing
        avg.SetHalfLifeTime(TDuration::Seconds(10));
        now += TDuration::Seconds(1);
        avg.Push(100, now);
        UNIT_ASSERT(avg.GetValue() < 100);
        avg.SetHalfLifeTime(TDuration::Zero());
        now += TDuration::MilliSeconds(1);
        avg.Push(42, now);
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 42, Eps);
    }

    Y_UNIT_TEST(Clear) {
        TExponentialMovingAverageValue<double> avg(TDuration::Seconds(1));
        avg.Push(100, TInstant::Seconds(1));
        avg.Clear();
        UNIT_ASSERT(!avg.IsValueReady());
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 0, Eps);
        // first push after Clear seeds the value again
        avg.Push(7, TInstant::Seconds(2));
        UNIT_ASSERT_DOUBLES_EQUAL(avg.GetValue(), 7, Eps);
    }

    Y_UNIT_TEST(WorksWithTuples) {
        using TValueType = std::tuple<double, double, double>;
        TExponentialMovingAverageValue<TValueType> avg(TDuration::Seconds(1));

        avg.Push({100.0, 100.0, 100.0}, TInstant::Seconds(1));
        UNIT_ASSERT(avg.IsValueReady());
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<0>(avg.GetValue()), 100.0, Eps);
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<1>(avg.GetValue()), 100.0, Eps);
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<2>(avg.GetValue()), 100.0, Eps);

        avg.Push({20.0, 180.0, 0.0}, TInstant::Seconds(3));
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<0>(avg.GetValue()), 40.0, Eps);
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<1>(avg.GetValue()), 160.0, Eps);
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<2>(avg.GetValue()), 25.0, Eps);

        avg.Clear();
        UNIT_ASSERT(!avg.IsValueReady());
        UNIT_ASSERT_DOUBLES_EQUAL(std::get<0>(avg.GetValue()), 0.0, Eps);
    }

}

} // NMetrics
} // NKikimr
