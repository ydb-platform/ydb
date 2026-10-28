// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/logger_provider_builder_context.h"
#include "opentelemetry/sdk/configuration/logger_provider_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{
class LoggerProvider;
}  // namespace logs

namespace configuration
{

/**
 * Builds a logger provider from a logger provider configuration model.
 */
class LoggerProviderBuilder
{
public:
  LoggerProviderBuilder()                                              = default;
  LoggerProviderBuilder(LoggerProviderBuilder &&)                      = default;
  LoggerProviderBuilder(const LoggerProviderBuilder &)                 = default;
  LoggerProviderBuilder &operator=(LoggerProviderBuilder &&)           = default;
  LoggerProviderBuilder &operator=(const LoggerProviderBuilder &other) = default;
  virtual ~LoggerProviderBuilder()                                     = default;

  virtual std::shared_ptr<opentelemetry::sdk::logs::LoggerProvider> Build(
      const LoggerProviderBuilderContext &context,
      const opentelemetry::sdk::configuration::LoggerProviderConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
