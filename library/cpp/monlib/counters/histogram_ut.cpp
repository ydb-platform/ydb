#include "histogram.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(THistorgamTest) {
    Y_UNIT_TEST(TakeSnapshot) {
        THdrHistogram h(1, 10, 3);
        UNIT_ASSERT(h.RecordValue(1));
        UNIT_ASSERT(h.RecordValue(2));
        UNIT_ASSERT(h.RecordValues(3, 10));
        UNIT_ASSERT(h.RecordValue(4));
        UNIT_ASSERT(h.RecordValue(5));

        UNIT_ASSERT_EQUAL(h.GetTotalCount(), 14);

        THistogramSnapshot snapshot;
        h.TakeSnaphot(&snapshot);

        UNIT_ASSERT_EQUAL(h.GetTotalCount(), 0);

        UNIT_ASSERT_EQUAL(snapshot.Min, 1);
        UNIT_ASSERT_EQUAL(snapshot.Max, 5);

        // >>> a = [1, 2] + [3 for i in range(10)]  + [4, 5]
        // >>> numpy.mean(a)
        // 3.0
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot.Mean, 3.0, 1e-6);

        // >>> numpy.std(a)
        // 0.84515425472851657
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot.StdDeviation, 0.84515425472851657, 1e-6);

        // >>> [(p, round(numpy.percentile(a, p))) for p in [50, 75, 90, 95, 98, 99, 99.9, 100]]
        // [(50, 3.0), (75, 3.0), (90, 4.0), (95, 4.0), (98, 5.0), (99, 5.0), (99.9, 5.0), (100, 5.0)]
        UNIT_ASSERT_EQUAL(snapshot.Percentile50, 3);
        UNIT_ASSERT_EQUAL(snapshot.Percentile75, 3);
        UNIT_ASSERT_EQUAL(snapshot.Percentile90, 4);
        UNIT_ASSERT_EQUAL(snapshot.Percentile95, 4);
        UNIT_ASSERT_EQUAL(snapshot.Percentile98, 5);
        UNIT_ASSERT_EQUAL(snapshot.Percentile99, 5);
        UNIT_ASSERT_EQUAL(snapshot.Percentile999, 5);

        UNIT_ASSERT_EQUAL(snapshot.TotalCount, 14);
    }
}
