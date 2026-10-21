// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/trace/samplers/composable_rule_based.h"

#include <string>
#include <utility>
#include <vector>

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

namespace
{
std::string BuildDescription(const std::vector<PredicatedSampler> &rules)
{
  std::string description = "ComposableRuleBasedSampler{";
  bool first              = true;
  for (const auto &rule : rules)
  {
    if (!first)
    {
      description += ",";
    }
    description += std::string(rule.sampler->GetDescription());
    first = false;
  }
  description += "}";
  return description;
}
}  // namespace

ComposableRuleBasedSampler::ComposableRuleBasedSampler(std::vector<PredicatedSampler> rules)
    : rules_(std::move(rules)), description_(BuildDescription(rules_))
{}

SamplingIntent ComposableRuleBasedSampler::GetSamplingIntent(
    const opentelemetry::trace::SpanContext &parent_context,
    opentelemetry::trace::TraceId trace_id,
    nostd::string_view name,
    opentelemetry::trace::SpanKind span_kind,
    const opentelemetry::common::KeyValueIterable &attributes,
    const opentelemetry::trace::SpanContextKeyValueIterable &links) noexcept
{
  for (const auto &rule : rules_)
  {
    if (rule.predicate->SpanMatches(parent_context, name, span_kind, attributes, links))
    {
      return rule.sampler->GetSamplingIntent(parent_context, trace_id, name, span_kind, attributes,
                                             links);
    }
  }

  SamplingIntent intent;
  intent.has_threshold      = false;
  intent.threshold_reliable = false;
  return intent;
}

nostd::string_view ComposableRuleBasedSampler::GetDescription() const noexcept
{
  return description_;
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
