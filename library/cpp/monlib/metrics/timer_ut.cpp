#include "timer.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/threading/future/future.h>

using namespace NMonitoring;
using namespace NThreading;

Y_UNIT_TEST_SUITE(TTimerTest) {

    using namespace std::chrono;

    struct TTestClock {
        using time_point = time_point<high_resolution_clock>;

        static time_point TimePoint;

        static time_point now() {
            return TimePoint;
        }
    };

    TTestClock::time_point TTestClock::TimePoint;


    Y_UNIT_TEST(Gauge) {
        TTestClock::TimePoint = TTestClock::time_point::min();

        TGauge gauge(0);
        {
            TMetricTimerScope<TGauge, milliseconds, TTestClock> t{&gauge};
            TTestClock::TimePoint += milliseconds(10);
        }
        UNIT_ASSERT_EQUAL(10, gauge.Get());

        {
            TMetricTimerScope<TGauge, milliseconds, TTestClock> t{&gauge};
            TTestClock::TimePoint += milliseconds(20);
        }
        UNIT_ASSERT_EQUAL(20, gauge.Get());
    }

    Y_UNIT_TEST(IntGauge) {
        TTestClock::TimePoint = TTestClock::time_point::min();

        TIntGauge gauge(0);
        {
            TMetricTimerScope<TIntGauge, milliseconds, TTestClock> t{&gauge};
            TTestClock::TimePoint += milliseconds(10);
        }
        UNIT_ASSERT_EQUAL(10, gauge.Get());

        {
            TMetricTimerScope<TIntGauge, milliseconds, TTestClock> t{&gauge};
            TTestClock::TimePoint += milliseconds(20);
        }
        UNIT_ASSERT_EQUAL(20, gauge.Get());
    }

    Y_UNIT_TEST(CounterNew) {
        TTestClock::TimePoint = TTestClock::time_point::min();

        TCounter counter(0);
        {
            TMetricTimerScope<TCounter, milliseconds, TTestClock> t{&counter};
            TTestClock::TimePoint += milliseconds(10);
        }
        UNIT_ASSERT_EQUAL(10, counter.Get());

        {
            TMetricTimerScope<TCounter, milliseconds, TTestClock> t{&counter};
            TTestClock::TimePoint += milliseconds(20);
        }
        UNIT_ASSERT_EQUAL(30, counter.Get());
    }

    Y_UNIT_TEST(Rate) {
        TTestClock::TimePoint = TTestClock::time_point::min();

        TRate rate(0);
        {
            TMetricTimerScope<TRate, milliseconds, TTestClock> t{&rate};
            TTestClock::TimePoint += milliseconds(10);
        }
        UNIT_ASSERT_EQUAL(10, rate.Get());

        {
            TMetricTimerScope<TRate, milliseconds, TTestClock> t{&rate};
            TTestClock::TimePoint += milliseconds(20);
        }
        UNIT_ASSERT_EQUAL(30, rate.Get());
    }

    Y_UNIT_TEST(Histogram) {
        TTestClock::TimePoint = TTestClock::time_point::min();

        auto assertHistogram = [](const TVector<ui64>& expected, IHistogramSnapshotPtr snapshot) {
            UNIT_ASSERT_EQUAL(expected.size(), snapshot->Count());
            for (size_t i = 0; i < expected.size(); ++i) {
                UNIT_ASSERT_EQUAL(expected[i], snapshot->Value(i));
            }
        };

        THistogram histogram(ExplicitHistogram({10, 20, 30}), true);
        {
            TMetricTimerScope<THistogram, milliseconds, TTestClock> t{&histogram};
            TTestClock::TimePoint += milliseconds(5);
        }
        assertHistogram({1, 0, 0, 0}, histogram.TakeSnapshot());

        {
            TMetricTimerScope<THistogram, milliseconds, TTestClock> t{&histogram};
            TTestClock::TimePoint += milliseconds(15);
        }
        assertHistogram({1, 1, 0, 0}, histogram.TakeSnapshot());
    }

    Y_UNIT_TEST(Moving) {
        TTestClock::TimePoint = TTestClock::time_point::min();

        TCounter counter(0);
        {
            TMetricTimerScope<TCounter, milliseconds, TTestClock> t{&counter};
            [tt = std::move(t)] {
                TTestClock::TimePoint += milliseconds(5);
                Y_UNUSED(tt);
            }();

            TTestClock::TimePoint += milliseconds(10);
        }

        UNIT_ASSERT_EQUAL(counter.Get(), 5);
    }

    Y_UNIT_TEST(MovingIntoApply) {
        TTestClock::TimePoint = TTestClock::time_point::min();
        auto pool = CreateThreadPool(1);

        TCounter counter(0);
        {
            TFutureFriendlyTimer<TCounter, milliseconds, TTestClock> t{&counter};

            auto f = Async([=] {
                return;
            }, *pool).Apply([tt = t] (auto) {
                TTestClock::TimePoint += milliseconds(5);
                tt.Record();
            });

            f.Wait();
            TTestClock::TimePoint += milliseconds(10);
        }

        UNIT_ASSERT_EQUAL(counter.Get(), 5);
    }
}
