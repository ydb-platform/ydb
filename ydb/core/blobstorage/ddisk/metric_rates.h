#pragma once

#include <util/datetime/base.h>
#include <library/cpp/time_provider/monotonic.h>
#include <array>
#include <optional>
#include <vector>

namespace NKikimr::NDDisk {

struct TDDiskMonRate {
    double Iops = 0;
    double BytesPerSecond = 0;
};

struct TDDiskMonRateSample {
    TInstant Timestamp;
    // DDisk Read/Write/Sync and shared DirectIO Read/Write.
    // Missing rates represent unavailable intervals.
    std::array<std::optional<TDDiskMonRate>, 5> Rates;
};

struct TDDiskMonCounterSample {
    TInstant Timestamp;
    TMonotonic SampledAt;
    std::array<std::array<ui64, 2>, 5> Counters;
};

std::vector<TDDiskMonRateSample> CalculateDDiskMonRateHistory(
    const std::vector<TDDiskMonCounterSample>& samples);

std::optional<TDDiskMonRate> CalculateDDiskMonRate(ui64 previousRequests, ui64 previousBytes,
    ui64 requests, ui64 bytes, TDuration elapsed);

} // namespace NKikimr::NDDisk
