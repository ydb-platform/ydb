#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <chrono>
#include <cstddef>
#include <memory>

namespace NYdb::inline Dev::NObservability {

struct TMetricBufferSettings {
    std::chrono::milliseconds FlushInterval = std::chrono::milliseconds(100);

    std::size_t ThreadPendingThreshold = 65536;

    std::size_t HistogramReserveSamples = 256;

    std::shared_ptr<NMetrics::IMetricRegistry> SelfMetricsRegistry;
};

std::shared_ptr<NMetrics::IMetricRegistry> CreateBufferedMetricRegistry(
    std::shared_ptr<NMetrics::IMetricRegistry> underlying,
    TMetricBufferSettings settings = {});

} // namespace NYdb::NObservability
