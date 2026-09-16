// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/trace/samplers/composable_probability.h"

#include <cstdint>
#include <string>

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

#include "ot_trace_state.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

ComposableProbabilitySampler::ComposableProbabilitySampler(double ratio)
{
  if (ratio > 1.0)
  {
    ratio = 1.0;
  }
  if (ratio < 0.0)
  {
    ratio = 0.0;
  }
  uint64_t threshold = CalculateThreshold(ratio);
  // A threshold of kMaxThreshold means 0% sampling. The specification asks for
  // this to behave like ComposableAlwaysOff: a non-probabilistic drop.
  has_threshold_ = threshold < kMaxThreshold;
  threshold_     = has_threshold_ ? threshold : 0;
  description_   = "ComposableProbabilitySampler{" + std::to_string(ratio) + "}";
}

SamplingIntent ComposableProbabilitySampler::GetSamplingIntent(
    const opentelemetry::trace::SpanContext & /* parent_context */,
    opentelemetry::trace::TraceId /* trace_id */,
    nostd::string_view /* name */,
    opentelemetry::trace::SpanKind /* span_kind */,
    const opentelemetry::common::KeyValueIterable & /* attributes */,
    const opentelemetry::trace::SpanContextKeyValueIterable & /* links */) noexcept
{
  SamplingIntent intent;
  intent.has_threshold      = has_threshold_;
  intent.threshold_value    = threshold_;
  intent.threshold_reliable = has_threshold_;
  return intent;
}

nostd::string_view ComposableProbabilitySampler::GetDescription() const noexcept
{
  return description_;
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
