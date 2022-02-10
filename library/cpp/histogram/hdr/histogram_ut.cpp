#include "histogram.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/cast.h>

using namespace NHdr;

void LoadData(THistogram* h1, THistogram* h2) {
    for (int i = 0; i < 1000; i++) {
        UNIT_ASSERT(h1->RecordValue(1000));
        UNIT_ASSERT(h2->RecordValueWithExpectedInterval(1000, 1000));
    }

    UNIT_ASSERT(h1->RecordValue(1000 * 1000));
    UNIT_ASSERT(h2->RecordValueWithExpectedInterval(1000 * 1000, 1000));
}

Y_UNIT_TEST_SUITE(THistogramTest) {
    Y_UNIT_TEST(Creation) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT_EQUAL(h.GetMemorySize(), 188512);
        UNIT_ASSERT_EQUAL(h.GetCountsLen(), 23552);
    }

    Y_UNIT_TEST(CreateWithLargeValues) {
        THistogram h(20L * 1000 * 1000, 100L * 1000 * 1000, 5);
        UNIT_ASSERT(h.RecordValue(100L * 1000 * 1000));
        UNIT_ASSERT(h.RecordValue(20L * 1000 * 1000));
        UNIT_ASSERT(h.RecordValue(30L * 1000 * 1000));

        i64 v50 = h.GetValueAtPercentile(50.0);
        i64 v8333 = h.GetValueAtPercentile(83.33);
        i64 v8334 = h.GetValueAtPercentile(83.34);
        i64 v99 = h.GetValueAtPercentile(99.0);

        UNIT_ASSERT_EQUAL(v50, 33554431);
        UNIT_ASSERT_EQUAL(v8333, 33554431);
        UNIT_ASSERT_EQUAL(v8334, 100663295);
        UNIT_ASSERT_EQUAL(v99, 100663295);

        UNIT_ASSERT(h.ValuesAreEqual(v50, 20L * 1000 * 1000));
        UNIT_ASSERT(h.ValuesAreEqual(v8333, 30L * 1000 * 1000));
        UNIT_ASSERT(h.ValuesAreEqual(v8334, 100L * 1000 * 1000));
        UNIT_ASSERT(h.ValuesAreEqual(v99, 100L * 1000 * 1000));
    }

    Y_UNIT_TEST(InvalidSignificantValueDigits) {
        UNIT_ASSERT_EXCEPTION(THistogram(1000, -1), yexception);
        UNIT_ASSERT_EXCEPTION(THistogram(1000, 0), yexception);
        UNIT_ASSERT_EXCEPTION(THistogram(1000, 6), yexception);
    }

    Y_UNIT_TEST(InvalidLowestDiscernibleValue) {
        UNIT_ASSERT_EXCEPTION(THistogram(0, 100, 3), yexception);
        UNIT_ASSERT_EXCEPTION(THistogram(110, 100, 3), yexception);
    }

    Y_UNIT_TEST(TotalCount) {
        i64 oneHour = SafeIntegerCast<i64>(TDuration::Hours(1).MicroSeconds());
        THistogram h1(oneHour, 3);
        THistogram h2(oneHour, 3);
        LoadData(&h1, &h2);

        UNIT_ASSERT_EQUAL(h1.GetTotalCount(), 1001);
        UNIT_ASSERT_EQUAL(h2.GetTotalCount(), 2000);
    }

    Y_UNIT_TEST(StatsValues) {
        i64 oneHour = SafeIntegerCast<i64>(TDuration::Hours(1).MicroSeconds());
        THistogram h1(oneHour, 3);
        THistogram h2(oneHour, 3);
        LoadData(&h1, &h2);

        // h1 - histogram without correction
        {
            UNIT_ASSERT_EQUAL(h1.GetMin(), 1000);
            UNIT_ASSERT_EQUAL(h1.GetMax(), 1000447);
            UNIT_ASSERT(h1.ValuesAreEqual(h1.GetMax(), 1000 * 1000));

            // >>> numpy.mean([1000 for i in range(1000)] + [1000000])
            // 1998.0019980019979
            UNIT_ASSERT_DOUBLES_EQUAL(h1.GetMean(), 1998, 0.5);

            // >>> numpy.std([1000 for i in range(1000)] + [1000000])
            // 31559.59423085126
            UNIT_ASSERT_DOUBLES_EQUAL(h1.GetStdDeviation(), 31559, 10);
        }

        // h2 - histogram with correction of co-ordinated omission
        {
            UNIT_ASSERT_EQUAL(h2.GetMin(), 1000);
            UNIT_ASSERT_EQUAL(h2.GetMax(), 1000447);
            UNIT_ASSERT(h2.ValuesAreEqual(h1.GetMax(), 1000 * 1000));
            UNIT_ASSERT_DOUBLES_EQUAL(h2.GetMean(), 250752, 0.5);
            UNIT_ASSERT_DOUBLES_EQUAL(h2.GetStdDeviation(), 322557, 0.5);
        }
    }

    Y_UNIT_TEST(Percentiles) {
        i64 oneHour = SafeIntegerCast<i64>(TDuration::Hours(1).MicroSeconds());
        THistogram h1(oneHour, 3);
        THistogram h2(oneHour, 3);
        LoadData(&h1, &h2);

        // >>> a = [1000 for i in range(1000)] + [1000000]
        // >>> [(p, numpy.percentile(a, p)) for p in [50, 90, 99, 99.99, 99.999, 100]]
        // [(50, 1000.0), (90, 1000.0), (99, 1000.0), (99.99, 900099.99999986368), (99.999, 990009.99999989558), (100, 1000000.0)]

        // h1 - histogram without correction
        {
            i64 v50 = h1.GetValueAtPercentile(50);
            i64 v90 = h1.GetValueAtPercentile(90);
            i64 v99 = h1.GetValueAtPercentile(99);
            i64 v9999 = h1.GetValueAtPercentile(99.99);
            i64 v99999 = h1.GetValueAtPercentile(99.999);
            i64 v100 = h1.GetValueAtPercentile(100);

            UNIT_ASSERT_EQUAL(v50, 1000);
            UNIT_ASSERT_EQUAL(v90, 1000);
            UNIT_ASSERT_EQUAL(v99, 1000);
            UNIT_ASSERT_EQUAL(v9999, 1000447);
            UNIT_ASSERT_EQUAL(v99999, 1000447);
            UNIT_ASSERT_EQUAL(v100, 1000447);

            UNIT_ASSERT(h1.ValuesAreEqual(v50, 1000));
            UNIT_ASSERT(h1.ValuesAreEqual(v90, 1000));
            UNIT_ASSERT(h1.ValuesAreEqual(v99, 1000));
            UNIT_ASSERT(h1.ValuesAreEqual(v9999, 1000 * 1000));
            UNIT_ASSERT(h1.ValuesAreEqual(v99999, 1000 * 1000));
            UNIT_ASSERT(h1.ValuesAreEqual(v100, 1000 * 1000));
        }

        // h2 - histogram with correction of co-ordinated omission
        {
            i64 v50 = h2.GetValueAtPercentile(50);
            i64 v90 = h2.GetValueAtPercentile(90);
            i64 v99 = h2.GetValueAtPercentile(99);
            i64 v9999 = h2.GetValueAtPercentile(99.99);
            i64 v99999 = h2.GetValueAtPercentile(99.999);
            i64 v100 = h2.GetValueAtPercentile(100);

            UNIT_ASSERT_EQUAL(v50, 1000);
            UNIT_ASSERT_EQUAL(v90, 800255);
            UNIT_ASSERT_EQUAL(v99, 980479);
            UNIT_ASSERT_EQUAL(v9999, 1000447);
            UNIT_ASSERT_EQUAL(v99999, 1000447);
            UNIT_ASSERT_EQUAL(v100, 1000447);

            UNIT_ASSERT(h2.ValuesAreEqual(v50, 1000));
            UNIT_ASSERT(h2.ValuesAreEqual(v90, 800 * 1000));
            UNIT_ASSERT(h2.ValuesAreEqual(v99, 980 * 1000));
            UNIT_ASSERT(h2.ValuesAreEqual(v9999, 1000 * 1000));
            UNIT_ASSERT(h2.ValuesAreEqual(v99999, 1000 * 1000));
            UNIT_ASSERT(h2.ValuesAreEqual(v100, 1000 * 1000));
        }
    }

    Y_UNIT_TEST(OutOfRangeValues) {
        THistogram h(1000, 4);
        UNIT_ASSERT(h.RecordValue(32767));
        UNIT_ASSERT(!h.RecordValue(32768));
    }

    Y_UNIT_TEST(Reset) {
        THistogram h(TDuration::Hours(1).MicroSeconds(), 3);
        UNIT_ASSERT(h.RecordValues(1000, 1000));
        UNIT_ASSERT(h.RecordValue(1000 * 1000));
        UNIT_ASSERT_EQUAL(h.GetTotalCount(), 1001);

        h.Reset();

        UNIT_ASSERT_EQUAL(h.GetTotalCount(), 0);
        UNIT_ASSERT_EQUAL(h.GetMin(), Max<i64>());
        UNIT_ASSERT_EQUAL(h.GetMax(), 0);
        UNIT_ASSERT_EQUAL(h.GetValueAtPercentile(99.0), 0);
    }
}
