// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/log_record_exporter_configuration.h"
#include "opentelemetry/sdk/configuration/log_record_processor_configuration.h"
#include "opentelemetry/sdk/configuration/logger_configurator_configuration.h"
#include "opentelemetry/sdk/configuration/registry.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/logs/exporter.h"
#include "opentelemetry/sdk/logs/logger_config.h"
#include "opentelemetry/sdk/logs/processor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

/**
 * Common utility functions for building logs SDK components from configuration models
 */
class LogsBuilderUtils
{
public:
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> CreateLogRecordExporter(
      const Registry *registry,
      const std::unique_ptr<LogRecordExporterConfiguration> &model);

  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordProcessor> CreateLogRecordProcessor(
      const Registry *registry,
      const std::unique_ptr<LogRecordProcessorConfiguration> &model);

  static std::unique_ptr<opentelemetry::sdk::instrumentationscope::ScopeConfigurator<
      opentelemetry::sdk::logs::LoggerConfig>>
  CreateLoggerConfigurator(const Registry *registry,
                           const std::unique_ptr<LoggerConfiguratorConfiguration> &model);
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
