// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/composable_parent_threshold_sampler_configuration.h"
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

class ComposableParentThresholdSamplerBuilder
{
public:
  ComposableParentThresholdSamplerBuilder()                                           = default;
  ComposableParentThresholdSamplerBuilder(ComposableParentThresholdSamplerBuilder &&) = default;
  ComposableParentThresholdSamplerBuilder(const ComposableParentThresholdSamplerBuilder &) =
      default;
  ComposableParentThresholdSamplerBuilder &operator=(ComposableParentThresholdSamplerBuilder &&) =
      default;
  ComposableParentThresholdSamplerBuilder &operator=(
      const ComposableParentThresholdSamplerBuilder &other) = default;
  virtual ~ComposableParentThresholdSamplerBuilder()        = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> Build(
      const opentelemetry::sdk::configuration::ComposableParentThresholdSamplerConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> &&root) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
