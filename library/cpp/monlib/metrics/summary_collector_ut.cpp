#include "summary_collector.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

#include <numeric>
#include <algorithm>

namespace NMonitoring {

Y_UNIT_TEST_SUITE(SummaryCollectorTest) {

    void CheckSnapshot(ISummaryDoubleSnapshotPtr snapshot, const TVector<double> values) {
        const double eps = 1e-9;

        double sum = std::accumulate(values.begin(), values.end(), 0.0);
        double min = *std::min_element(values.begin(), values.end());
        double max = *std::max_element(values.begin(), values.end());
        double last = values.back();
        ui64 count = values.size();

        UNIT_ASSERT_DOUBLES_EQUAL(snapshot->GetSum(), sum, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot->GetMin(), min, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot->GetMax(), max, eps);
        UNIT_ASSERT_DOUBLES_EQUAL(snapshot->GetLast(), last, eps);
        UNIT_ASSERT_EQUAL(snapshot->GetCount(), count);
    }

    Y_UNIT_TEST(Simple) {
        {
            TVector<double> test{05, -1.5, 0.0, 2.5, 0.25, -1.0};
            TSummaryDoubleCollector summary;
            for (auto value : test) {
                summary.Collect(value);
            }
            CheckSnapshot(summary.Snapshot(), test);
        }
        {
            TVector<double> test{-1.0, 1.0, 9.0, -5000.0, 5000.0, 5.0, -5.0};
            TSummaryDoubleCollector summary;
            for (auto value : test) {
                summary.Collect(value);
            }
            CheckSnapshot(summary.Snapshot(), test);
        }
    }

    Y_UNIT_TEST(RandomStressTest) {
        const ui32 attemts = 100;
        for (ui32 i = 0; i < attemts; ++i) {
            const ui32 size = 100;
            TVector<double> values(size);
            TSummaryDoubleCollector summary;
            for (auto& value : values) {
                value = RandomNumber<double>() - 0.5;
                summary.Collect(value);
            }
            CheckSnapshot(summary.Snapshot(), values);
        }
    }
}

}
