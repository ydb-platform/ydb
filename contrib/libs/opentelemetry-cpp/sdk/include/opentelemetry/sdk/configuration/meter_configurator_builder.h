// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/meter_configurator_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace instrumentationscope
{
template <typename T>
class ScopeConfigurator;
}  // namespace instrumentationscope

namespace metrics
{
class MeterConfig;
}  // namespace metrics

namespace configuration
{

class MeterConfiguratorBuilder
{
public:
  MeterConfiguratorBuilder()                                                 = default;
  MeterConfiguratorBuilder(MeterConfiguratorBuilder &&)                      = default;
  MeterConfiguratorBuilder(const MeterConfiguratorBuilder &)                 = default;
  MeterConfiguratorBuilder &operator=(MeterConfiguratorBuilder &&)           = default;
  MeterConfiguratorBuilder &operator=(const MeterConfiguratorBuilder &other) = default;
  virtual ~MeterConfiguratorBuilder()                                        = default;

  virtual std::unique_ptr<opentelemetry::sdk::instrumentationscope::ScopeConfigurator<
      opentelemetry::sdk::metrics::MeterConfig>>
  Build(const opentelemetry::sdk::configuration::MeterConfiguratorConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
