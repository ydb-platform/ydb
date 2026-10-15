#include "metric_rates.h"

namespace NKikimr::NDDisk {

std::optional<TDDiskMonRate> CalculateDDiskMonRate(ui64 previousRequests, ui64 previousBytes,
        ui64 requests, ui64 bytes, TDuration elapsed) {
    if (!elapsed || requests < previousRequests || bytes < previousBytes) {
        return std::nullopt;
    }
    return TDDiskMonRate{double(requests - previousRequests) / elapsed.SecondsFloat(),
        double(bytes - previousBytes) / elapsed.SecondsFloat()};
}

std::vector<TDDiskMonRateSample> CalculateDDiskMonRateHistory(
        const std::vector<TDDiskMonCounterSample>& samples) {
    std::vector<TDDiskMonRateSample> result;
    result.reserve(samples.size());
    const TDDiskMonCounterSample* previous = nullptr;
    for (const auto& sample : samples) {
        TDDiskMonRateSample rate;
        rate.Timestamp = sample.Timestamp;
        if (previous && sample.SampledAt > previous->SampledAt) {
            const auto elapsed = sample.SampledAt - previous->SampledAt;
            if (elapsed <= TDuration::Seconds(2)) {
                for (size_t i = 0; i < rate.Rates.size(); ++i) {
                    rate.Rates[i] = CalculateDDiskMonRate(previous->Counters[i][0], previous->Counters[i][1],
                        sample.Counters[i][0], sample.Counters[i][1], elapsed);
                }
            }
        }
        result.push_back(rate);
        previous = &sample;
    }
    return result;
}

} // namespace NKikimr::NDDisk
