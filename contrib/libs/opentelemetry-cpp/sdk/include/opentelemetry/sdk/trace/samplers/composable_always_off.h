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
 * ComposableAlwaysOff returns a SamplingIntent with no threshold, which the
 * CompositeSampler interprets as "drop". threshold_reliable is false so
 * downstream collectors cannot derive a sampling probability from this
 * sampler's decision.
 */
class ComposableAlwaysOffSampler : public ComposableSampler
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
    intent.has_threshold      = false;
    intent.threshold_reliable = false;
    return intent;
  }

  nostd::string_view GetDescription() const noexcept override
  {
    return "ComposableAlwaysOffSampler";
  }
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
