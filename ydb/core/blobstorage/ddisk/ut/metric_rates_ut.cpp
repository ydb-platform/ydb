#include <ydb/core/blobstorage/ddisk/metric_rates.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk {
Y_UNIT_TEST_SUITE(TDDiskMetricRates) {
    Y_UNIT_TEST(OperationHistoryUsesMonotonicIntervalsAndPreservesGapsAndResets) {
        std::vector<TDDiskMonCounterSample> samples(6);
        for (size_t i = 0; i < samples.size(); ++i) {
            samples[i].Timestamp = TInstant::Seconds(100 + i);
            samples[i].SampledAt = TMonotonic::MilliSeconds(1000 + i * 1500);
            for (auto& counter : samples[i].Counters) {
                counter = {10 + i * 3, 100 + i * 300};
            }
        }
        // A wall-clock step must not change rates.
        samples[1].Timestamp = TInstant::Seconds(103);
        samples[2].Counters[1] = {0, 0};
        // Long missing intervals and non-increasing clock readings are unavailable.
        samples[3].SampledAt = samples[2].SampledAt + TDuration::Seconds(3);
        samples[4].SampledAt = samples[3].SampledAt;
        samples[5].SampledAt = samples[4].SampledAt + TDuration::Seconds(1);
        samples[5].Counters = samples[4].Counters;
        const auto history = CalculateDDiskMonRateHistory(samples);
        UNIT_ASSERT(!history[0].Rates[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[1].Rates[0]->Iops, 2);
        UNIT_ASSERT_VALUES_EQUAL(history[1].Rates[0]->BytesPerSecond, 200);
        UNIT_ASSERT(!history[2].Rates[1]);
        UNIT_ASSERT(history[2].Rates[0]);
        UNIT_ASSERT(!history[3].Rates[0]);
        UNIT_ASSERT(!history[4].Rates[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[5].Rates[0]->Iops, 0);
        UNIT_ASSERT_VALUES_EQUAL(history[5].Rates[0]->BytesPerSecond, 0);
    }

}
}
