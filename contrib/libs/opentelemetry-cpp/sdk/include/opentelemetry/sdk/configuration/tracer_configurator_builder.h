// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/tracer_configurator_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace instrumentationscope
{
template <typename T>
class ScopeConfigurator;
}  // namespace instrumentationscope

namespace trace
{
class TracerConfig;
}  // namespace trace

namespace configuration
{

class TracerConfiguratorBuilder
{
public:
  TracerConfiguratorBuilder()                                                  = default;
  TracerConfiguratorBuilder(TracerConfiguratorBuilder &&)                      = default;
  TracerConfiguratorBuilder(const TracerConfiguratorBuilder &)                 = default;
  TracerConfiguratorBuilder &operator=(TracerConfiguratorBuilder &&)           = default;
  TracerConfiguratorBuilder &operator=(const TracerConfiguratorBuilder &other) = default;
  virtual ~TracerConfiguratorBuilder()                                         = default;

  virtual std::unique_ptr<opentelemetry::sdk::instrumentationscope::ScopeConfigurator<
      opentelemetry::sdk::trace::TracerConfig>>
  Build(const opentelemetry::sdk::configuration::TracerConfiguratorConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
