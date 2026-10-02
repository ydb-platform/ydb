// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/meter_provider_builder_context.h"
#include "opentelemetry/sdk/configuration/meter_provider_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class MeterProvider;
}  // namespace metrics

namespace configuration
{

/**
 * Builds a meter provider from a meter provider configuration model.
 */
class MeterProviderBuilder
{
public:
  MeterProviderBuilder()                                             = default;
  MeterProviderBuilder(MeterProviderBuilder &&)                      = default;
  MeterProviderBuilder(const MeterProviderBuilder &)                 = default;
  MeterProviderBuilder &operator=(MeterProviderBuilder &&)           = default;
  MeterProviderBuilder &operator=(const MeterProviderBuilder &other) = default;
  virtual ~MeterProviderBuilder()                                    = default;

  virtual std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> Build(
      const MeterProviderBuilderContext &context,
      const opentelemetry::sdk::configuration::MeterProviderConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
