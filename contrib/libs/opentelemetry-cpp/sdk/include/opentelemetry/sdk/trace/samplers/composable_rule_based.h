// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "opentelemetry/sdk/trace/samplers/composable_sampler.h"
#include "opentelemetry/sdk/trace/samplers/predicate.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * A (Predicate, ComposableSampler) pair evaluated by ComposableRuleBasedSampler.
 */
struct PredicatedSampler
{
  std::shared_ptr<Predicate> predicate;
  std::shared_ptr<ComposableSampler> sampler;
};

/**
 * ComposableRuleBasedSampler evaluates its rules in order and returns the
 * SamplingIntent of the first sampler whose predicate matches. When no rule
 * matches it returns a drop intent.
 */
class ComposableRuleBasedSampler : public ComposableSampler
{
public:
  explicit ComposableRuleBasedSampler(std::vector<PredicatedSampler> rules);

  SamplingIntent GetSamplingIntent(
      const opentelemetry::trace::SpanContext &parent_context,
      opentelemetry::trace::TraceId trace_id,
      nostd::string_view name,
      opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept override;

  nostd::string_view GetDescription() const noexcept override;

private:
  std::vector<PredicatedSampler> rules_;
  std::string description_;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
