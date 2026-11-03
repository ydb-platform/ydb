// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/composable_always_off_sampler_configuration.h"
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

class ComposableAlwaysOffSamplerBuilder
{
public:
  ComposableAlwaysOffSamplerBuilder()                                                = default;
  ComposableAlwaysOffSamplerBuilder(ComposableAlwaysOffSamplerBuilder &&)            = default;
  ComposableAlwaysOffSamplerBuilder(const ComposableAlwaysOffSamplerBuilder &)       = default;
  ComposableAlwaysOffSamplerBuilder &operator=(ComposableAlwaysOffSamplerBuilder &&) = default;
  ComposableAlwaysOffSamplerBuilder &operator=(const ComposableAlwaysOffSamplerBuilder &other) =
      default;
  virtual ~ComposableAlwaysOffSamplerBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> Build(
      const opentelemetry::sdk::configuration::ComposableAlwaysOffSamplerConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
