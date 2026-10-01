// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/composable_probability_sampler_configuration.h"
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

class ComposableProbabilitySamplerBuilder
{
public:
  ComposableProbabilitySamplerBuilder()                                                  = default;
  ComposableProbabilitySamplerBuilder(ComposableProbabilitySamplerBuilder &&)            = default;
  ComposableProbabilitySamplerBuilder(const ComposableProbabilitySamplerBuilder &)       = default;
  ComposableProbabilitySamplerBuilder &operator=(ComposableProbabilitySamplerBuilder &&) = default;
  ComposableProbabilitySamplerBuilder &operator=(const ComposableProbabilitySamplerBuilder &other) =
      default;
  virtual ~ComposableProbabilitySamplerBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> Build(
      const opentelemetry::sdk::configuration::ComposableProbabilitySamplerConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
