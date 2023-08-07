#include "percentile.h"
#include "percentile_lg.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/cputimer.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(PercentileTest) {

template<size_t A, size_t B, size_t B_BEGIN>
void PrintSizeAndLimit() {
    using TPerc = TPercentileTrackerLg<A, B, 15>;
    Cout << "TPercentileTrackerLg<" << A << ", " << B << ", 15>"
        << "; sizeof# " << LeftPad(HumanReadableSize(sizeof(TPerc), SF_BYTES), 7)
        << "; max_granularity# " << LeftPad(HumanReadableSize(TPerc::MAX_GRANULARITY, SF_QUANTITY), 5)
        << "; limit# " << LeftPad(HumanReadableSize(TPerc::TRACKER_LIMIT , SF_QUANTITY), 5) << Endl;
    if constexpr (B > 1) {
        PrintSizeAndLimit<A, B - 1, B_BEGIN>();
    } else if constexpr (A > 1) {
        Cout << Endl;
        PrintSizeAndLimit<A - 1, B_BEGIN, B_BEGIN>();
    }
}

    Y_UNIT_TEST(PrintTrackerLgSizeAndLimits) {
        PrintSizeAndLimit<10, 5, 5>();
    }

template<class T>
void RunPerf() {
    TTimer t(TypeName<T>() + "\n");
    T tracker;
    for (size_t i = 0; i < 1000000; ++i) {
        for (size_t i = 0; i < 10; ++i) {
            tracker.Increment(i * 6451);
        }
        tracker.Update();
    }
}

    Y_UNIT_TEST(TrackerPerf) {
        RunPerf<TPercentileTracker<4, 512, 15>>();
        RunPerf<TPercentileTrackerLg<4, 3, 15>>();
    }

    Y_UNIT_TEST(TrackerLimitTest) {
        {
            using TPerc = TPercentileTrackerLg<1, 0, 1>;
            TPerc tracker;
            tracker.Increment(Max<size_t>());
            UNIT_ASSERT_EQUAL(TPerc::TRACKER_LIMIT, tracker.GetPercentile(1.0));
        }
        {
            using TPerc = TPercentileTrackerLg<1, 1, 1>;
            TPerc tracker;
            tracker.Increment(Max<size_t>());
            UNIT_ASSERT_EQUAL(TPerc::TRACKER_LIMIT, tracker.GetPercentile(1.0));
        }
        {
            using TPerc = TPercentileTrackerLg<1, 5, 1>;
            TPerc tracker;
            tracker.Increment(Max<size_t>());
            UNIT_ASSERT_EQUAL(TPerc::TRACKER_LIMIT, tracker.GetPercentile(1.0));
        }
        {
            using TPerc = TPercentileTrackerLg<2, 1, 1>;
            TPerc tracker;
            tracker.Increment(Max<size_t>());
            UNIT_ASSERT_EQUAL(TPerc::TRACKER_LIMIT, tracker.GetPercentile(1.0));
        }
        {
            using TPerc = TPercentileTrackerLg<5, 4, 1>;
            TPerc tracker;
            tracker.Increment(Max<size_t>());
            UNIT_ASSERT_EQUAL(TPerc::TRACKER_LIMIT, tracker.GetPercentile(1.0));
        }
    }

    Y_UNIT_TEST(BucketIdxIfvsBucketIdxBinarySearch) {
        for (size_t var = 0; var < 5; var++) {
            if (var == 0) {
                TPercentileTrackerLg<3, 2, 15> tracker;
                for (size_t i = 0; i < 3000000; i += 1) {
                    size_t num1 = tracker.BucketIdxMostSignificantBit(i);
                    size_t num2 = tracker.BucketIdxBinarySearch(i);
                    UNIT_ASSERT_EQUAL(num1, num2);
                }
            } else if (var == 1) {
                TPercentileTrackerLg<4, 4, 15> tracker;
                for (size_t i = 0; i < 3000000; i += 1) {
                    size_t num1 = tracker.BucketIdxMostSignificantBit(i);
                    size_t num2 = tracker.BucketIdxBinarySearch(i);
                    UNIT_ASSERT_EQUAL(num1, num2);
                }
            } else if (var == 2) {
                TPercentileTrackerLg<5, 3, 15> tracker;
                for (size_t i = 0; i < 3000000; i += 1) {
                    size_t num1 = tracker.BucketIdxMostSignificantBit(i);
                    size_t num2 = tracker.BucketIdxBinarySearch(i);
                    size_t num3 = tracker.BucketIdxIf(i);
                    UNIT_ASSERT_EQUAL(num1, num2);
                    UNIT_ASSERT_EQUAL(num2, num3);
                }
            } else if (var == 3) {
                TPercentileTrackerLg<5, 4, 15> tracker;
                for (size_t i = 0; i < 3000000; i += 1) {
                    size_t num1 = tracker.BucketIdxMostSignificantBit(i);
                    size_t num2 = tracker.BucketIdxBinarySearch(i);
                    size_t num3 = tracker.BucketIdxIf(i);
                    UNIT_ASSERT_EQUAL(num1, num2);
                    UNIT_ASSERT_EQUAL(num2, num3);
                }
            } else if (var == 4) {
                TPercentileTrackerLg<6, 5, 15> tracker;
                for (size_t i = 0; i < 3000000; i += 1) {
                    size_t num1 = tracker.BucketIdxMostSignificantBit(i);
                    size_t num2 = tracker.BucketIdxBinarySearch(i);
                    UNIT_ASSERT_EQUAL(num1, num2);
                }
                for (size_t i = 0; i < 400000000000ul; i += 1303) {
                    size_t num1 = tracker.BucketIdxMostSignificantBit(i);
                    size_t num2 = tracker.BucketIdxBinarySearch(i);
                    UNIT_ASSERT_EQUAL(num1, num2);
                }
            }
        }
    }

    Y_UNIT_TEST(DifferentPercentiles) {
        TPercentileTrackerLg<5, 4, 15> tracker;
        TVector<size_t> values({0, 115, 1216, 15, 3234567, 1234567, 216546, 263421, 751654, 96, 224, 223, 225});
        TVector<size_t> percentiles50({0, 0, 116, 15, 116, 116, 1216, 1216, 217056, 1216, 1216, 224, 232});
        TVector<size_t> percentiles75({0, 116, 116, 116, 1216, 1245152, 217056, 270304, 753632, 753632,
                270304, 270304, 270304});
        TVector<size_t> percentiles90({ 0, 116, 1216, 1216, 2064352, 1245152, 1245152, 1245152, 1245152,
                1245152, 1245152, 1245152, 1245152});
        TVector<size_t> percentiles100({ 0, 116, 1216, 1216, 2064352, 2064352, 2064352, 2064352, 2064352,
                2064352, 2064352, 2064352, 2064352 });

        for (size_t i = 0; i < values.size(); ++i) {
            tracker.Increment(values[i]);
            UNIT_ASSERT_EQUAL(tracker.GetPercentile(0.5), percentiles50[i]);
            UNIT_ASSERT_EQUAL(tracker.GetPercentile(0.75), percentiles75[i]);
            UNIT_ASSERT_EQUAL(tracker.GetPercentile(0.90), percentiles90[i]);
            UNIT_ASSERT_EQUAL(tracker.GetPercentile(1.0), percentiles100[i]);
        }
    }
}
