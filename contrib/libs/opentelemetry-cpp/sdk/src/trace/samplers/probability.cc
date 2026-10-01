// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <atomic>
#include <cstdint>
#include <map>
#include <string>

#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/probability.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/trace_flags.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/trace/trace_state.h"
#include "opentelemetry/version.h"

#include "ot_trace_state.h"

namespace trace_api = opentelemetry::trace;

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

ProbabilitySampler::ProbabilitySampler(double ratio)
{
  const double valid_ratio = ValidateRatio(ratio, "ProbabilitySampler");
  description_             = "ProbabilitySampler{" + std::to_string(valid_ratio) + "}";
  threshold_               = CalculateThreshold(valid_ratio);
}

SamplingResult ProbabilitySampler::ShouldSample(
    const trace_api::SpanContext &parent_context,
    trace_api::TraceId trace_id,
    nostd::string_view /*name*/,
    trace_api::SpanKind /*span_kind*/,
    const opentelemetry::common::KeyValueIterable & /*attributes*/,
    const trace_api::SpanContextKeyValueIterable & /*links*/) noexcept
{
  const auto &parent_trace_state = parent_context.trace_state();
  const bool has_parent_entries  = parent_trace_state && !parent_trace_state->Empty();

  OtelTraceState ot_state;
  if (has_parent_entries)
  {
    std::string ot_value;
    if (parent_trace_state->Get(kOtTraceStateKey, ot_value))
    {
      ot_state = OtelTraceState::Parse(ot_value);
    }
  }

  bool drop = threshold_ == kMaxThreshold;
  if (!drop)
  {
    uint64_t randomness = 0;
    if (ot_state.has_random_value)
    {
      // An explicit "rv" from an upstream Level 2 participant wins over the
      // trace id, keeping the sampling decision consistent across the trace.
      randomness = ot_state.random_value;
    }
    else
    {
      if (parent_context.IsValid() && !parent_context.trace_flags().IsRandom())
      {
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true))
        {
          OTEL_INTERNAL_LOG_WARN(
              "ProbabilitySampler presumes TraceID randomness, but the W3C random trace flag is "
              "not set. Upgrade the caller to W3C Trace Context Level 2.");
        }
      }
      randomness = GetRandomnessFromTraceId(trace_id);
    }
    drop = randomness < threshold_;
  }

  // Record the effective threshold when sampling; a dropped span carries no
  // probability, so its inherited (now stale) "th" must be erased. The "rv"
  // sub-key and any other "ot" sub-keys are preserved by OtelTraceState.
  if (drop)
  {
    ot_state.has_threshold = false;
  }
  else
  {
    ot_state.has_threshold = true;
    ot_state.threshold     = threshold_;
  }

  std::string new_ot_value = ot_state.Serialize();
  auto trace_state =
      has_parent_entries ? parent_trace_state->Delete(kOtTraceStateKey) : parent_trace_state;
  if (!new_ot_value.empty())
  {
    if (!trace_state)
    {
      trace_state = trace_api::TraceState::GetDefault();
    }
    trace_state = trace_state->Set(kOtTraceStateKey, new_ot_value);
  }

  return {drop ? Decision::DROP : Decision::RECORD_AND_SAMPLE, nullptr, trace_state};
}

nostd::string_view ProbabilitySampler::GetDescription() const noexcept
{
  return description_;
}
}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
