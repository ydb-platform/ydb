// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/extension_composable_sampler_configuration.h"
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

class ExtensionComposableSamplerBuilder
{
public:
  ExtensionComposableSamplerBuilder()                                                = default;
  ExtensionComposableSamplerBuilder(ExtensionComposableSamplerBuilder &&)            = default;
  ExtensionComposableSamplerBuilder(const ExtensionComposableSamplerBuilder &)       = default;
  ExtensionComposableSamplerBuilder &operator=(ExtensionComposableSamplerBuilder &&) = default;
  ExtensionComposableSamplerBuilder &operator=(const ExtensionComposableSamplerBuilder &other) =
      default;
  virtual ~ExtensionComposableSamplerBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::ComposableSampler> Build(
      const opentelemetry::sdk::configuration::ExtensionComposableSamplerConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
