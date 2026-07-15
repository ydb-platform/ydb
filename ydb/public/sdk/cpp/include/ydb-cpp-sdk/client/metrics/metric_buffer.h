#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <chrono>
#include <cstddef>
#include <memory>

namespace NYdb::inline Dev::NObservability {

inline constexpr auto kDefaultFlushInterval = std::chrono::milliseconds(100);
inline constexpr std::size_t kDefaultThreadPendingThreshold = 64 * 1024;
inline constexpr std::size_t kDefaultThreadPendingLimit =
    4 * kDefaultThreadPendingThreshold;
inline constexpr std::size_t kDefaultHistogramReserveSamples = 256;

struct TMetricBufferSettings {
    std::chrono::milliseconds FlushInterval = kDefaultFlushInterval;
    std::size_t ThreadPendingThreshold = kDefaultThreadPendingThreshold;
    std::size_t ThreadPendingLimit = kDefaultThreadPendingLimit;
    std::size_t HistogramReserveSamples = kDefaultHistogramReserveSamples;

    std::shared_ptr<NMetrics::IMetricRegistry> SelfMetricsRegistry;
};

std::shared_ptr<NMetrics::IMetricRegistry> CreateBufferedMetricRegistry(
    std::shared_ptr<NMetrics::IMetricRegistry> underlying,
    TMetricBufferSettings settings = {});

bool FlushBufferedMetricRegistry(const std::shared_ptr<NMetrics::IMetricRegistry>& registry);

} // namespace NYdb::NObservability
