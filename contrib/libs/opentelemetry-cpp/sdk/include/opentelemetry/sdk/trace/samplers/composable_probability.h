// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <string>

#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * ComposableProbabilitySampler samples a configurable fraction of traces
 * using consistent probability sampling. It corresponds to the specification's
 * ComposableProbability sampler and the ComposableProbabilitySamplerConfiguration
 * config model.
 *
 * The ratio maps to a 56-bit rejection threshold. A ratio of 0 drops every span
 * (an unreliable, non-probabilistic intent, equivalent to ComposableAlwaysOff).
 */
class ComposableProbabilitySampler : public ComposableSampler
{
public:
  explicit ComposableProbabilitySampler(double ratio);

  SamplingIntent GetSamplingIntent(
      const opentelemetry::trace::SpanContext &parent_context,
      opentelemetry::trace::TraceId trace_id,
      nostd::string_view name,
      opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept override;

  nostd::string_view GetDescription() const noexcept override;

private:
  std::string description_;
  uint64_t threshold_;
  bool has_threshold_;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
