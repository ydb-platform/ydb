// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <string>

#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * ComposableParentThresholdSampler honors the sampling decision of the parent
 * span. For root spans it delegates to the configured root sampler.
 *
 * For non-root spans it propagates the parent's threshold from the incoming "ot"
 * tracestate when present. Otherwise it falls back to the parent's sampled flag:
 * a sampled parent yields a keep-all intent (marked unreliable, since the
 * sampled flag carries no adjusted-count information), a non-sampled parent
 * yields a drop intent.
 */
class ComposableParentThresholdSampler : public ComposableSampler
{
public:
  explicit ComposableParentThresholdSampler(std::shared_ptr<ComposableSampler> root_sampler);

  SamplingIntent GetSamplingIntent(
      const opentelemetry::trace::SpanContext &parent_context,
      opentelemetry::trace::TraceId trace_id,
      nostd::string_view name,
      opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept override;

  nostd::string_view GetDescription() const noexcept override;

private:
  std::shared_ptr<ComposableSampler> root_sampler_;
  std::string description_;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
