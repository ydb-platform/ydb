#include "kqp_join_topology_generator.h"

#include <ydb/core/kqp/ut/common/kqp_benches.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpBenches) {

    void CheckStats(TRunningStatistics& stats,
                    ui32 n, double min, double max,
                    double median, double mad,
                    double q1, double q3, double iqr,
                    double mean, double stdev) {

        const double TOLERANCE = 0.0001;

        UNIT_ASSERT_EQUAL(stats.GetN(), n);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.GetMin(), min, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.GetMax(), max, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.GetMedian(), median, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(stats.CalculateMAD(), mad, TOLERANCE);

        auto statistics = stats.GetStatistics();
        UNIT_ASSERT_EQUAL(statistics.GetN(), n);
        UNIT_ASSERT_DOUBLES_EQUAL(statistics.GetMin(), min, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(statistics.GetMax(), max, TOLERANCE);

        auto report = statistics.ComputeStatistics();
        UNIT_ASSERT_EQUAL(report.N, n);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Min, min, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Max, max, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Median, median, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.MAD, mad, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Q1, q1, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Q3, q3, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.IQR, iqr, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Mean, mean, TOLERANCE);
        UNIT_ASSERT_DOUBLES_EQUAL(report.Stdev, stdev, TOLERANCE);
    }

    Y_UNIT_TEST(TStatistics) {
        TRunningStatistics stats;

        stats.AddValue(1);
        stats.AddValue(1);
        CheckStats(stats, 2, 1, 1, 1, 0, 1, 1, 0, 1, 0);

        stats.AddValue(1);
        CheckStats(stats, 3, 1, 1, 1, 0, 1, 1, 0, 1, 0);

        stats.AddValue(1);
        CheckStats(stats, 4, 1, 1, 1, 0, 1, 1, 0, 1, 0);

        stats.AddValue(3);
        CheckStats(stats, 5, 1, 3, 1, 0, 1, 1, 0, 1.4000, 0.8944);

        stats.AddValue(5);
        CheckStats(stats, 6, 1, 5, 1, 0, 1, 2.5000, 1.5000, 2, 1.6733);

        stats.AddValue(7);
        CheckStats(stats, 7, 1, 7, 1, 0, 1, 4, 3, 2.7143, 2.4300);

        stats.AddValue(7);
        CheckStats(stats, 8, 1, 7, 2, 1, 1, 5.5000, 4.5000, 3.2500, 2.7124);

        stats.AddValue(8);
        CheckStats(stats, 9, 1, 8, 3, 2, 1, 7, 6, 3.7778, 2.9907);

        stats.AddValue(100);
        CheckStats(stats, 10, 1, 100, 4, 3, 1, 7, 6, 13.4000, 30.5585);

        stats.AddValue(12);
        CheckStats(stats, 11, 1, 100, 5, 4, 1, 7.5000, 6.5000, 13.2727, 28.9934);

        stats.AddValue(15);
        CheckStats(stats, 12, 1, 100, 6, 5, 1, 9, 8, 13.4167, 27.6486);

        stats.AddValue(11);
        CheckStats(stats, 13, 1, 100, 7, 5, 1, 11, 10, 13.2308, 26.4800);

        stats.AddValue(18);
        CheckStats(stats, 14, 1, 100, 7, 5.5000, 1.5000, 11.7500, 10.2500, 13.5714, 25.4731);

        stats.AddValue(19);
        CheckStats(stats, 15, 1, 100, 7, 6, 2, 13.5000, 11.5000, 13.9333, 24.5865);

        stats.AddValue(14);
        CheckStats(stats, 16, 1, 100, 7.5000, 6.5000, 2.5000, 14.2500, 11.7500, 13.9375, 23.7528);

        stats.AddValue(21);
        CheckStats(stats, 17, 1, 100, 8, 7, 3, 15, 12, 14.3529, 23.0623);

        stats.AddValue(9);
        CheckStats(stats, 18, 1, 100, 8.5000, 6, 3.5000, 14.7500, 11.2500, 14.0556, 22.4092);

        stats.AddValue(25);
        CheckStats(stats, 19, 1, 100, 9, 6, 4, 16.5000, 12.5000, 14.6316, 21.9221);

        stats.AddValue(17);
        CheckStats(stats, 20, 1, 100, 10, 7, 4.5000, 17.2500, 12.7500, 14.7500, 21.3440);

        stats.AddValue(18);
        CheckStats(stats, 21, 1, 100, 11, 7, 5, 18, 13, 14.9048, 20.8156);

        stats.AddValue(10);
        CheckStats(stats, 22, 1, 100, 10.5000, 7, 5.5000, 17.7500, 12.2500, 14.6818, 20.3409);

        stats.AddValue(22);
        CheckStats(stats, 23, 1, 100, 11, 7, 6, 18, 12, 15, 19.9317);
    }

} // Y_UNIT_TEST_SUITE(KqpBenches)

} // namespace NKikimr::NKqp
