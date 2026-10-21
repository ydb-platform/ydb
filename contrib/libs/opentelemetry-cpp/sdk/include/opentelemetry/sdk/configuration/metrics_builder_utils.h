// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/include_exclude_configuration.h"
#include "opentelemetry/sdk/configuration/meter_configurator_configuration.h"
#include "opentelemetry/sdk/configuration/metric_reader_configuration.h"
#include "opentelemetry/sdk/configuration/registry.h"
#include "opentelemetry/sdk/configuration/view_configuration.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/metrics/meter_config.h"
#include "opentelemetry/sdk/metrics/metric_reader.h"
#include "opentelemetry/sdk/metrics/view/attributes_processor.h"
#include "opentelemetry/sdk/metrics/view/view_registry.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

/**
 * Common utility functions for building metrics SDK components from configuration models.
 */
class MetricsBuilderUtils
{
public:
  static std::unique_ptr<opentelemetry::sdk::metrics::MetricReader> CreateMetricReader(
      const Registry *registry,
      const std::unique_ptr<MetricReaderConfiguration> &model);

  static void AddView(opentelemetry::sdk::metrics::ViewRegistry *view_registry,
                      const std::unique_ptr<ViewConfiguration> &model);

  static std::unique_ptr<opentelemetry::sdk::metrics::AttributesProcessor>
  CreateAttributesProcessor(const std::unique_ptr<IncludeExcludeConfiguration> &model);

  static std::unique_ptr<opentelemetry::sdk::instrumentationscope::ScopeConfigurator<
      opentelemetry::sdk::metrics::MeterConfig>>
  CreateMeterConfigurator(const Registry *registry,
                          const std::unique_ptr<MeterConfiguratorConfiguration> &model);
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
