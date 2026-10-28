// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/composable_always_on_sampler_configuration.h"
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

class ComposableAlwaysOnSamplerBuilder
{
public:
  ComposableAlwaysOnSamplerBuilder()                                               = default;
  ComposableAlwaysOnSamplerBuilder(ComposableAlwaysOnSamplerBuilder &&)            = default;
  ComposableAlwaysOnSamplerBuilder(const ComposableAlwaysOnSamplerBuilder &)       = default;
  ComposableAlwaysOnSamplerBuilder &operator=(ComposableAlwaysOnSamplerBuilder &&) = default;
  ComposableAlwaysOnSamplerBuilder &operator=(const ComposableAlwaysOnSamplerBuilder &other) =
      default;
  virtual ~ComposableAlwaysOnSamplerBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> Build(
      const opentelemetry::sdk::configuration::ComposableAlwaysOnSamplerConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
