// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/metrics/metric_reader.h"
#include <mutex>
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/metrics/cardinality_limits.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

void MetricReader::SetMetricProducer(MetricProducer *metric_producer)
{
  metric_producer_ = metric_producer;
  OnInitialized();
}

bool MetricReader::Collect(
    nostd::function_ref<bool(ResourceMetrics &metric_data)> callback) noexcept
{
  if (!metric_producer_)
  {
    OTEL_INTERNAL_LOG_WARN(
        "MetricReader::Collect Cannot invoke Produce(). No MetricProducer registered for "
        "collection!")
    return false;
  }
  if (IsShutdown())
  {
    // Continue with warning, and let pull and push MetricReader state machine handle this.
    OTEL_INTERNAL_LOG_WARN("MetricReader::Collect invoked while Shutdown in progress!");
  }

  auto result = metric_producer_->Produce();

  // According to the spec,
  //   When the Produce operation fails, the MetricProducer MAY return successfully collected
  //   results and a failed reasons list to the caller.
  // So we invoke the callback with whatever points we get back, even if the overall operation may
  // have failed.
  auto success = callback(result.points_);
  return (result.status_ == MetricProducer::Status::kSuccess) && success;
}

bool MetricReader::Shutdown(std::chrono::microseconds timeout) noexcept
{
  // Serialize so concurrent calls block until the first call's shutdown has completed.
  std::lock_guard<std::mutex> shutdown_guard{shutdown_m_};
  if (shutdown_.exchange(true, std::memory_order_release))
  {
    OTEL_INTERNAL_LOG_WARN("MetricReader::Shutdown - Already shutdown!");
    return true;
  }
  bool status = true;

  if (!OnShutDown(timeout))
  {
    status = false;
    OTEL_INTERNAL_LOG_WARN("MetricReader::OnShutDown Shutdown failed. Will not be tried again!");
  }
  return status;
}

/** Flush metric read by this reader **/
bool MetricReader::ForceFlush(std::chrono::microseconds timeout) noexcept
{
  if (IsShutdown())
  {
    OTEL_INTERNAL_LOG_WARN(
        "MetricReader::ForceFlush Cannot invoke Force flush on shutdown reader!");
    return false;
  }

  bool status = true;
  if (!OnForceFlush(timeout))
  {
    status = false;
    OTEL_INTERNAL_LOG_ERROR("MetricReader::OnForceFlush failed!");
  }
  return status;
}

bool MetricReader::IsShutdown() const noexcept
{
  return shutdown_.load(std::memory_order_acquire);
}

std::size_t MetricReader::GetCardinalityLimit(InstrumentType instrument_type) const noexcept
{
  switch (instrument_type)
  {
    case InstrumentType::kCounter:
      return cardinality_limits_.counter;
    case InstrumentType::kHistogram:
      return cardinality_limits_.histogram;
    case InstrumentType::kUpDownCounter:
      return cardinality_limits_.up_down_counter;
    case InstrumentType::kObservableCounter:
      return cardinality_limits_.observable_counter;
    case InstrumentType::kObservableGauge:
      return cardinality_limits_.observable_gauge;
    case InstrumentType::kObservableUpDownCounter:
      return cardinality_limits_.observable_up_down_counter;
    case InstrumentType::kGauge:
      return cardinality_limits_.gauge;
    default:
      return cardinality_limits_.default_limit;
  }
}

void MetricReader::SetCardinalityLimits(const CardinalityLimits &limits) noexcept
{
  cardinality_limits_ = limits;
  // TODO: Reader-level limits are stored but not yet enforced as a per-collector
  // fallback during the collection path. Enforcement will be added in a follow-up.
  OTEL_INTERNAL_LOG_WARN(
      "MetricReader::SetCardinalityLimits - reader-level cardinality limits are stored "
      "but not yet enforced during collection. Use view-level AggregationConfig to "
      "enforce limits today.");
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
