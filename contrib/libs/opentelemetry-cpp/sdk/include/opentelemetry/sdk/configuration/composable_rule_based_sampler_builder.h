// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <vector>

#include "opentelemetry/sdk/configuration/composable_rule_based_sampler_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
class ComposableSampler;
}  // namespace trace

namespace configuration
{

class ComposableRuleBasedSamplerBuilder
{
public:
  ComposableRuleBasedSamplerBuilder()                                                = default;
  ComposableRuleBasedSamplerBuilder(ComposableRuleBasedSamplerBuilder &&)            = default;
  ComposableRuleBasedSamplerBuilder(const ComposableRuleBasedSamplerBuilder &)       = default;
  ComposableRuleBasedSamplerBuilder &operator=(ComposableRuleBasedSamplerBuilder &&) = default;
  ComposableRuleBasedSamplerBuilder &operator=(const ComposableRuleBasedSamplerBuilder &other) =
      default;
  virtual ~ComposableRuleBasedSamplerBuilder() = default;

  /// rule_samplers is index-aligned with model->rules. Entries are null for
  /// rules with no sampler, which implementations must skip.
  virtual std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> Build(
      const opentelemetry::sdk::configuration::ComposableRuleBasedSamplerConfiguration *model,
      std::vector<std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler>> &&rule_samplers)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
