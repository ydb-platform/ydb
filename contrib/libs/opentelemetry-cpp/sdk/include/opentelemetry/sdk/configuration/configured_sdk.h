// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/configuration/configuration.h"
#include "opentelemetry/sdk/configuration/registry.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{
class LoggerProvider;
}  // namespace logs
namespace metrics
{
class MeterProvider;
}  // namespace metrics
namespace trace
{
class TracerProvider;
}  // namespace trace

namespace configuration
{

/**
 * This class represents a fully configured SDK.
 * A SDK contains various objects, like propagators and providers for each
 * signals, that collectively describe the opentelemetry configuration.
 */
class ConfiguredSdk
{
public:
  static std::unique_ptr<ConfiguredSdk> Create(
      std::shared_ptr<Registry> registry,
      const std::unique_ptr<opentelemetry::sdk::configuration::Configuration> &model);

  ConfiguredSdk() : resource(opentelemetry::sdk::resource::Resource::GetEmpty()) {}

  /**
   * Install the SDK, so that an instrumented application can make calls
   * to it.
   * This methods sets the global provider singletons to point to the SDK.
   */
  void Install();

  /**
   * Uninstall the SDK, so that an instrumented application no longer makes
   * calls to it.
   * This method clears the global provider singletons.
   */
  void UnInstall();

  opentelemetry::sdk::common::internal_log::LogLevel log_level{
      opentelemetry::sdk::common::internal_log::LogLevel::Info};
  opentelemetry::sdk::resource::Resource resource;
  std::shared_ptr<opentelemetry::sdk::trace::TracerProvider> tracer_provider;
  std::shared_ptr<opentelemetry::context::propagation::TextMapPropagator> propagator;
  std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider;
  std::shared_ptr<opentelemetry::sdk::logs::LoggerProvider> logger_provider;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
