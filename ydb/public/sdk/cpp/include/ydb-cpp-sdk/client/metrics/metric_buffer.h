#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <chrono>
#include <cstddef>
#include <memory>

namespace NYdb::inline Dev::NObservability {

// Configuration for NObservability::TMetricBuffer.
//
// TMetricBuffer is an in-process aggregation layer that sits between
// the SDK's hot path (TStatCollector / TRequestMetrics) and the user-
// supplied IMetricRegistry (typically the OpenTelemetry-backed plugin).
// Each application thread accumulates metric updates into its own
// thread-local buckets keyed by the underlying metric handle pointer;
// a single background thread periodically (`FlushInterval`) drains all
// buckets and pushes the accumulated state via the *batched* entry
// points on ICounter::Add(uint64_t) / IHistogram::RecordMany().
//
// Goal: reduce contention on the shared OTel aggregator (one mutex /
// atomic op per Inc()/Record() call) by coalescing many hot-path
// updates into one batched call to the registry.
//
// This is the "batch processing of telemetry data" component sitting
// *before* the OTel export pipeline (which itself batches *after* the
// aggregator via PeriodicExportingMetricReader / BatchSpanProcessor).
struct TMetricBufferSettings {
    // How often the background thread flushes accumulated per-thread
    // updates to the underlying registry.
    std::chrono::milliseconds FlushInterval = std::chrono::milliseconds(100);

    // If non-zero, the flush is also triggered as soon as any single
    // thread has accumulated this many pending updates (across all
    // counters and histograms). Lets the buffer self-regulate under
    // bursty load without growing thread-local buffers unboundedly.
    std::size_t ThreadPendingThreshold = 65536;

    // Initial reservation for the per-thread histogram sample buffers.
    // Larger values trade memory for fewer reallocations on hot paths.
    std::size_t HistogramReserveSamples = 256;

    // Where the buffer publishes its own diagnostic metrics
    // (ydb_sdk_metric_buffer_*). Defaults to the wrapped registry —
    // these metrics bypass the buffer itself to avoid recursion.
    std::shared_ptr<NMetrics::IMetricRegistry> SelfMetricsRegistry;
};

// Wraps `underlying` with an in-process aggregation layer. The returned
// IMetricRegistry transparently buffers per-thread updates and flushes
// them to `underlying` on a timer; from the caller's perspective the
// semantics of Counter()/Histogram()/Gauge() are unchanged.
//
// Notes:
//  * Counters: Inc() and Add(N) are buffered as a per-thread uint64
//    accumulator; on flush a single Add(total) call is made.
//  * Histograms: Record() / RecordMany() append to a per-thread
//    sample vector; on flush a single RecordMany(samples) call is
//    made.
//  * Gauges: Set() is buffered last-wins (only the most recent value
//    per thread is flushed); Add(delta) goes straight to the
//    underlying gauge to preserve atomicity of read-modify-write.
//  * On registry destruction the buffer is gracefully shut down: it
//    performs one final synchronous drain so no updates are lost.
std::shared_ptr<NMetrics::IMetricRegistry> CreateBufferedMetricRegistry(
    std::shared_ptr<NMetrics::IMetricRegistry> underlying,
    TMetricBufferSettings settings = {});

} // namespace NYdb::NObservability
