// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace common
{
class KeyValueIterable;
}  // namespace common

namespace trace
{
class SpanContext;
class SpanContextKeyValueIterable;
class TraceState;
}  // namespace trace

namespace sdk
{
namespace trace
{

/**
 * Span attributes optionally provided by a ComposableSampler when the span
 * is kept. Returning an empty map means no extra attributes.
 */
using SamplingIntentAttributes = std::map<std::string, opentelemetry::common::AttributeValue>;

/**
 * Optional callback returning attributes to add when the span is recorded.
 * Invoked at most once, and only if the CompositeSampler keeps the span.
 */
using AttributesProvider = std::function<SamplingIntentAttributes()>;

/**
 * Optional callback returning a modified TraceState. The resulting TraceState
 * must not modify the "ot" sub-key: that is owned by the CompositeSampler.
 */
using TraceStateProvider = std::function<nostd::shared_ptr<opentelemetry::trace::TraceState>(
    nostd::shared_ptr<opentelemetry::trace::TraceState>)>;

/**
 * SamplingIntent describes a ComposableSampler's preference for a span.
 *
 * The threshold is a 56-bit value in the low bits of threshold_value. The
 * CompositeSampler keeps the span when the trace's 56-bit randomness R
 * satisfies R >= threshold_value. When has_threshold is false the span is
 * dropped unconditionally.
 *
 * When threshold_reliable is true the CompositeSampler writes the threshold
 * back to the "ot" tracestate "th:" sub-key, so downstream samplers and
 * collectors can reconstruct the sampling probability.
 */
struct SamplingIntent
{
  uint64_t threshold_value = 0;
  bool has_threshold       = false;
  bool threshold_reliable  = false;

  AttributesProvider attributes_provider{};
  TraceStateProvider trace_state_provider{};
};

/**
 * ComposableSampler is the building block of the CompositeSampler. It does
 * not itself decide DROP / RECORD_AND_SAMPLE: it returns a SamplingIntent
 * that the wrapping CompositeSampler turns into a SamplingResult.
 *
 * Implementations must not modify the "ot" tracestate sub-key: the
 * CompositeSampler owns its serialization.
 */
class ComposableSampler
{
public:
  ComposableSampler()                                     = default;
  ComposableSampler(const ComposableSampler &)            = delete;
  ComposableSampler(ComposableSampler &&)                 = delete;
  ComposableSampler &operator=(const ComposableSampler &) = delete;
  ComposableSampler &operator=(ComposableSampler &&)      = delete;

  virtual ~ComposableSampler() = default;

  /**
   * Compute a SamplingIntent for the span being created. Parameters match
   * Sampler::ShouldSample so existing Sampler implementations can be adapted.
   */
  virtual SamplingIntent GetSamplingIntent(
      const opentelemetry::trace::SpanContext &parent_context,
      opentelemetry::trace::TraceId trace_id,
      nostd::string_view name,
      opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept = 0;

  /**
   * Short description with configuration, for debug pages and logs.
   */
  virtual nostd::string_view GetDescription() const noexcept = 0;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
