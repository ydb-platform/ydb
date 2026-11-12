// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>

#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/sdk/metrics/cardinality_limits.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

/**
 * MetricReader defines the interface to collect metrics from SDK
 */
class MetricReader
{
public:
  MetricReader()                                = default;
  MetricReader(const MetricReader &)            = delete;
  MetricReader(MetricReader &&)                 = delete;
  MetricReader &operator=(const MetricReader &) = delete;
  MetricReader &operator=(MetricReader &&)      = delete;

  void SetMetricProducer(MetricProducer *metric_producer);

  /**
   * Collect the metrics from SDK.
   * @return return the status of the operation.
   */
  bool Collect(nostd::function_ref<bool(ResourceMetrics &metric_data)> callback) noexcept;

  /**
   * Get the AggregationTemporality for given Instrument Type for this reader.
   *
   * @return AggregationTemporality
   */

  virtual AggregationTemporality GetAggregationTemporality(
      InstrumentType instrument_type) const noexcept = 0;

  /**
   * Get the cardinality limit for a given instrument type.
   * Defaults to kAggregationCardinalityLimit (2000) unless overridden via
   * SetCardinalityLimits().
   *
   * @param instrument_type The instrument type to get the limit for
   * @return The cardinality limit for the requested instrument type
   */
  std::size_t GetCardinalityLimit(InstrumentType instrument_type) const noexcept;

  /**
   * Set per-instrument-type cardinality limits for this reader.
   *
   * TODO: Reader-level limits are stored but not yet enforced as a per-collector
   * fallback during the collection path. Enforcement will be added in a follow-up.
   * View-level limits (via AggregationConfig) are enforced today.
   *
   * @param limits The cardinality limits to apply
   */
  void SetCardinalityLimits(const CardinalityLimits &limits) noexcept;

  /**
   * Shutdown the metric reader.
   *
   * Idempotent and a completion barrier: only the first call runs OnShutDown(), and a
   * concurrent call blocks until the first call has finished before returning.
   *
   * @return the result of OnShutDown() for the first call, true for any subsequent call.
   */
  bool Shutdown(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept;

  /**
   * Force flush the metric read by the reader.
   *
   * @return false without invoking OnForceFlush() if the reader is already shut down, otherwise
   * the result of OnForceFlush().
   */
  bool ForceFlush(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept;

  /**
   * Return the status of Metric reader.
   */
  bool IsShutdown() const noexcept;

  virtual ~MetricReader() = default;

private:
  virtual bool OnForceFlush(std::chrono::microseconds timeout) noexcept = 0;

  virtual bool OnShutDown(std::chrono::microseconds timeout) noexcept = 0;

  virtual void OnInitialized() noexcept {}

protected:
private:
  MetricProducer *metric_producer_{nullptr};
  std::mutex shutdown_m_;
  std::atomic<bool> shutdown_{false};
  CardinalityLimits cardinality_limits_;
};
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
