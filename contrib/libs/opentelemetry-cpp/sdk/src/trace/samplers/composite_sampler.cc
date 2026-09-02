// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/trace/samplers/composite_sampler.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/trace/trace_state.h"
#include "opentelemetry/version.h"

#include "ot_trace_state.h"
#include "src/common/random.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

CompositeSampler::CompositeSampler(std::shared_ptr<ComposableSampler> delegate)
    : delegate_(std::move(delegate)),
      description_("CompositeSampler{" + std::string(delegate_->GetDescription()) + "}")
{}

SamplingResult CompositeSampler::ShouldSample(
    const opentelemetry::trace::SpanContext &parent_context,
    opentelemetry::trace::TraceId trace_id,
    nostd::string_view name,
    opentelemetry::trace::SpanKind span_kind,
    const opentelemetry::common::KeyValueIterable &attributes,
    const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept
{
  const auto &parent_trace_state = parent_context.trace_state();

  std::string ot_value;
  if (parent_trace_state)
  {
    parent_trace_state->Get(kOtTraceStateKey, ot_value);
  }
  OtelTraceState ot_state = OtelTraceState::Parse(ot_value);

  SamplingIntent intent =
      delegate_->GetSamplingIntent(parent_context, trace_id, name, span_kind, attributes, links);

  bool is_sampled = false;
  bool reliable   = false;
  if (intent.has_threshold)
  {
    reliable            = intent.threshold_reliable;
    uint64_t randomness = 0;
    if (reliable)
    {
      randomness =
          ot_state.has_random_value ? ot_state.random_value : GetRandomnessFromTraceId(trace_id);
    }
    else
    {
      // The decision does not carry a reliable adjusted count, so the trace
      // randomness must not be reused (the threshold may correlate with it).
      randomness = opentelemetry::sdk::common::Random::GenerateRandom64() & kMaxRandomValue;
    }
    is_sampled = intent.threshold_value <= randomness;
  }

  Decision decision = is_sampled ? Decision::RECORD_AND_SAMPLE : Decision::DROP;

  // Write the effective threshold back only when the span is kept with a
  // reliable adjusted count. Otherwise drop the "th" sub-key. The "rv" sub-key
  // and any other sub-keys are always preserved.
  if (is_sampled && reliable)
  {
    ot_state.has_threshold = true;
    ot_state.threshold     = intent.threshold_value;
  }
  else
  {
    ot_state.has_threshold = false;
  }
  std::string new_ot_value = ot_state.Serialize();

  nostd::shared_ptr<opentelemetry::trace::TraceState> out_trace_state =
      parent_trace_state ? parent_trace_state : opentelemetry::trace::TraceState::GetDefault();
  if (intent.trace_state_provider)
  {
    out_trace_state = intent.trace_state_provider(out_trace_state);
  }
  if (new_ot_value.empty())
  {
    out_trace_state = out_trace_state->Delete(kOtTraceStateKey);
  }
  else
  {
    out_trace_state = out_trace_state->Set(kOtTraceStateKey, new_ot_value);
  }

  std::unique_ptr<const std::map<std::string, opentelemetry::common::AttributeValue>>
      attributes_out;
  if (is_sampled && intent.attributes_provider)
  {
    SamplingIntentAttributes provided = intent.attributes_provider();
    if (!provided.empty())
    {
      attributes_out.reset(
          new std::map<std::string, opentelemetry::common::AttributeValue>(std::move(provided)));
    }
  }

  return {decision, std::move(attributes_out), out_trace_state};
}

nostd::string_view CompositeSampler::GetDescription() const noexcept
{
  return description_;
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
