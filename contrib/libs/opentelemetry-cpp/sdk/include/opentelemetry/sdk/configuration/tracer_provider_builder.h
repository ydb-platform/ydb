// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/tracer_provider_builder_context.h"
#include "opentelemetry/sdk/configuration/tracer_provider_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
class TracerProvider;
}  // namespace trace

namespace configuration
{

/**
 * Builds a tracer provider from a tracer provider configuration model.
 */
class TracerProviderBuilder
{
public:
  TracerProviderBuilder()                                              = default;
  TracerProviderBuilder(TracerProviderBuilder &&)                      = default;
  TracerProviderBuilder(const TracerProviderBuilder &)                 = default;
  TracerProviderBuilder &operator=(TracerProviderBuilder &&)           = default;
  TracerProviderBuilder &operator=(const TracerProviderBuilder &other) = default;
  virtual ~TracerProviderBuilder()                                     = default;

  virtual std::shared_ptr<opentelemetry::sdk::trace::TracerProvider> Build(
      const TracerProviderBuilderContext &context,
      const opentelemetry::sdk::configuration::TracerProviderConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
