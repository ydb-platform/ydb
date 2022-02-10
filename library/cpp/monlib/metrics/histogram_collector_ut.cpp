#include "histogram_collector.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(THistogramCollectorTest) {
    void CheckSnapshot(
        const IHistogramSnapshot& s,
        const TBucketBounds& bounds,
        const TBucketValues& values) {
        UNIT_ASSERT_VALUES_EQUAL(bounds.size(), values.size());
        UNIT_ASSERT_VALUES_EQUAL(bounds.size(), s.Count());

        double epsilon = std::numeric_limits<double>::epsilon();
        for (ui32 i = 0; i < s.Count(); i++) {
            UNIT_ASSERT_DOUBLES_EQUAL(bounds[i], s.UpperBound(i), epsilon);
            UNIT_ASSERT_VALUES_EQUAL(values[i], s.Value(i));
        }
    }

    Y_UNIT_TEST(Explicit) {
        auto histogram = ExplicitHistogram({0, 1, 2, 5, 10, 20});
        histogram->Collect(-2);
        histogram->Collect(-1);
        histogram->Collect(0);
        histogram->Collect(1);
        histogram->Collect(20);
        histogram->Collect(21);
        histogram->Collect(1000);

        TBucketBounds expectedBounds = {0, 1, 2, 5, 10, 20, Max<TBucketBound>()};
        TBucketValues expectedValues = {3, 1, 0, 0, 0, 1, 2};

        CheckSnapshot(*histogram->Snapshot(), expectedBounds, expectedValues);
    }

    Y_UNIT_TEST(ExplicitWithFloadBounds) {
        auto histogram = ExplicitHistogram({0.1, 0.5, 1, 1.7, 10});
        histogram->Collect(0.3, 2);
        histogram->Collect(0.01);
        histogram->Collect(0.9);
        histogram->Collect(0.6);
        histogram->Collect(1.1);
        histogram->Collect(0.7);
        histogram->Collect(2.71);

        TBucketBounds expectedBounds = {0.1, 0.5, 1, 1.7, 10, Max<TBucketBound>()};
        TBucketValues expectedValues = {1, 2, 3, 1, 1, 0};

        CheckSnapshot(*histogram->Snapshot(), expectedBounds, expectedValues);
    }

    Y_UNIT_TEST(Exponential) {
        auto histogram = ExponentialHistogram(6, 2.0, 3.0);
        histogram->Collect(-1);
        histogram->Collect(0);
        histogram->Collect(1);
        histogram->Collect(3);
        histogram->Collect(4);
        histogram->Collect(5);
        histogram->Collect(22);
        histogram->Collect(23);
        histogram->Collect(24);
        histogram->Collect(50);
        histogram->Collect(100);
        histogram->Collect(1000);

        TBucketBounds expectedBounds = {3, 6, 12, 24, 48, Max<TBucketBound>()};
        TBucketValues expectedValues = {4, 2, 0, 3, 0, 3};

        CheckSnapshot(*histogram->Snapshot(), expectedBounds, expectedValues);
    }

    Y_UNIT_TEST(Linear) {
        auto histogram = LinearHistogram(6, 5, 15);
        histogram->Collect(-1);
        histogram->Collect(0);
        histogram->Collect(1);
        histogram->Collect(4);
        histogram->Collect(5);
        histogram->Collect(6);
        histogram->Collect(64);
        histogram->Collect(65);
        histogram->Collect(66);
        histogram->Collect(100);
        histogram->Collect(1000);

        TBucketBounds expectedBounds = {5, 20, 35, 50, 65, Max<TBucketBound>()};
        TBucketValues expectedValues = {5, 1, 0, 0, 2, 3};

        CheckSnapshot(*histogram->Snapshot(), expectedBounds, expectedValues);
    }

    Y_UNIT_TEST(SnapshotOutput) {
        auto histogram = ExplicitHistogram({0, 1, 2, 5, 10, 20});
        histogram->Collect(-2);
        histogram->Collect(-1);
        histogram->Collect(0);
        histogram->Collect(1);
        histogram->Collect(20);
        histogram->Collect(21);
        histogram->Collect(1000);

        auto snapshot = histogram->Snapshot();

        TStringStream ss;
        ss << *snapshot;

        UNIT_ASSERT_STRINGS_EQUAL(
            "{0: 3, 1: 1, 2: 0, 5: 0, 10: 0, 20: 1, inf: 2}",
            ss.Str());
    }
}
