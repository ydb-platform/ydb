// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/trace/samplers/composable_parent_threshold.h"

#include <memory>
#include <string>
#include <utility>

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/trace/trace_state.h"
#include "opentelemetry/version.h"

#include "ot_trace_state.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

ComposableParentThresholdSampler::ComposableParentThresholdSampler(
    std::shared_ptr<ComposableSampler> root_sampler)
    : root_sampler_(std::move(root_sampler)),
      description_("ComposableParentThresholdSampler{" +
                   std::string(root_sampler_->GetDescription()) + "}")
{}

SamplingIntent ComposableParentThresholdSampler::GetSamplingIntent(
    const opentelemetry::trace::SpanContext &parent_context,
    opentelemetry::trace::TraceId trace_id,
    nostd::string_view name,
    opentelemetry::trace::SpanKind span_kind,
    const opentelemetry::common::KeyValueIterable &attributes,
    const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept
{
  if (!parent_context.IsValid())
  {
    return root_sampler_->GetSamplingIntent(parent_context, trace_id, name, span_kind, attributes,
                                            links);
  }

  std::string ot_value;
  const auto &parent_trace_state = parent_context.trace_state();
  if (parent_trace_state)
  {
    parent_trace_state->Get(kOtTraceStateKey, ot_value);
  }
  OtelTraceState ot_state = OtelTraceState::Parse(ot_value);

  SamplingIntent intent;
  if (ot_state.has_threshold)
  {
    intent.has_threshold      = true;
    intent.threshold_value    = ot_state.threshold;
    intent.threshold_reliable = true;
  }
  else if (parent_context.IsSampled())
  {
    // No reliable threshold from the parent; fall back to the sampled flag. The
    // adjusted count cannot be trusted because the sampled flag itself depends
    // on the trace randomness.
    intent.has_threshold      = true;
    intent.threshold_value    = 0;
    intent.threshold_reliable = false;
  }
  else
  {
    intent.has_threshold      = false;
    intent.threshold_reliable = false;
  }
  return intent;
}

nostd::string_view ComposableParentThresholdSampler::GetDescription() const noexcept
{
  return description_;
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
