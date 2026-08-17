// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <string>

#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * CompositeSampler adapts a ComposableSampler into a Sampler.
 *
 * It computes the span's 56-bit randomness (from the "ot" tracestate "rv:"
 * sub-key if present, otherwise from the low 56 bits of the TraceId), asks
 * the delegate for a SamplingIntent, and then:
 *
 *  - If the intent has a threshold and R >= threshold_value, returns
 *    RECORD_AND_SAMPLE; otherwise returns DROP.
 *  - If threshold_reliable is true, writes the hex-encoded threshold to the
 *    "ot" "th:" sub-key of the outgoing TraceState (trailing zeros trimmed,
 *    zero encoded as "0"). Otherwise the "th:" sub-key is removed.
 *  - Invokes attributes_provider only when the span is kept.
 *  - Applies trace_state_provider to the outgoing TraceState before writing
 *    back the "th:" sub-key.
 */
class CompositeSampler : public Sampler
{
public:
  explicit CompositeSampler(std::shared_ptr<ComposableSampler> delegate);

  SamplingResult ShouldSample(
      const opentelemetry::trace::SpanContext &parent_context,
      opentelemetry::trace::TraceId trace_id,
      nostd::string_view name,
      opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept override;

  nostd::string_view GetDescription() const noexcept override;

private:
  std::shared_ptr<ComposableSampler> delegate_;
  std::string description_;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
