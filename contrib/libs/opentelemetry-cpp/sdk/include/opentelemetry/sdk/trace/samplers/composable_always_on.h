// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * ComposableAlwaysOn returns a SamplingIntent that keeps every span.
 * Threshold is 0 (lowest, keeps everything) and threshold_reliable is true,
 * so the CompositeSampler writes "th:0" into the "ot" tracestate.
 */
class ComposableAlwaysOnSampler : public ComposableSampler
{
public:
  SamplingIntent GetSamplingIntent(
      const opentelemetry::trace::SpanContext & /*parent_context*/,
      opentelemetry::trace::TraceId /*trace_id*/,
      nostd::string_view /*name*/,
      opentelemetry::trace::SpanKind /*span_kind*/,
      const opentelemetry::common::KeyValueIterable & /*attributes*/,
      const opentelemetry::trace::SpanContextKeyValueIterable & /*links*/) noexcept override
  {
    SamplingIntent intent;
    intent.threshold_value    = 0;
    intent.has_threshold      = true;
    intent.threshold_reliable = true;
    return intent;
  }

  nostd::string_view GetDescription() const noexcept override
  {
    return "ComposableAlwaysOnSampler";
  }
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
