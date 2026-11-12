// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/logger_configurator_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace instrumentationscope
{
template <typename T>
class ScopeConfigurator;
}  // namespace instrumentationscope

namespace logs
{
class LoggerConfig;
}  // namespace logs

namespace configuration
{

class LoggerConfiguratorBuilder
{
public:
  LoggerConfiguratorBuilder()                                                  = default;
  LoggerConfiguratorBuilder(LoggerConfiguratorBuilder &&)                      = default;
  LoggerConfiguratorBuilder(const LoggerConfiguratorBuilder &)                 = default;
  LoggerConfiguratorBuilder &operator=(LoggerConfiguratorBuilder &&)           = default;
  LoggerConfiguratorBuilder &operator=(const LoggerConfiguratorBuilder &other) = default;
  virtual ~LoggerConfiguratorBuilder()                                         = default;

  virtual std::unique_ptr<opentelemetry::sdk::instrumentationscope::ScopeConfigurator<
      opentelemetry::sdk::logs::LoggerConfig>>
  Build(const opentelemetry::sdk::configuration::LoggerConfiguratorConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
