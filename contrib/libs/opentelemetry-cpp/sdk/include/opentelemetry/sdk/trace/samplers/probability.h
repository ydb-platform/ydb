// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <string>

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
/**
 * The ProbabilitySampler computes and returns a decision based on the
 * span's 56-bit randomness value and the configured ratio, following the
 * consistent probability sampling specification.
 */
class ProbabilitySampler : public Sampler
{
public:
  /**
   * @param ratio the sampling probability: either 0 (never sample) or a
   * value in [2^-56, 1.0]. Other values (including NaN) log a warning and
   * fall back to the default of 1.0. If the randomness value of the span is
   * greater than or equal to the rejection threshold derived from the ratio,
   * ShouldSample will return RECORD_AND_SAMPLE.
   */
  explicit ProbabilitySampler(double ratio);

  /**
   * @return Returns either RECORD_AND_SAMPLE or DROP based on the comparison
   * of the span's randomness value against the configured rejection
   * threshold. The parent SampledFlag is ignored.
   */
  SamplingResult ShouldSample(
      const opentelemetry::trace::SpanContext &parent_context,
      opentelemetry::trace::TraceId trace_id,
      nostd::string_view /*name*/,
      opentelemetry::trace::SpanKind /*span_kind*/,
      const opentelemetry::common::KeyValueIterable & /*attributes*/,
      const opentelemetry::trace::SpanContextKeyValueIterable & /*links*/) noexcept override;

  /**
   * @return Description MUST be ProbabilitySampler{0.000100}
   */
  nostd::string_view GetDescription() const noexcept override;

private:
  std::string description_;
  uint64_t threshold_;
};
}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
