// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "opentelemetry/sdk/configuration/cardinality_limits_configuration.h"
#include "opentelemetry/sdk/configuration/metric_producer_configuration.h"
#include "opentelemetry/sdk/configuration/metric_reader_configuration.h"
#include "opentelemetry/sdk/configuration/metric_reader_configuration_visitor.h"
#include "opentelemetry/sdk/configuration/push_metric_exporter_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/meter_provider.yaml
// YAML-NODE: PeriodicMetricReader
class PeriodicMetricReaderConfiguration : public MetricReaderConfiguration
{
public:
  // TODO: spec default is 60000ms for interval, using 5000ms to preserve original behavior
  static constexpr std::size_t kDefaultIntervalMs = 5000;
  static constexpr std::size_t kDefaultTimeoutMs  = 30000;

  void Accept(MetricReaderConfigurationVisitor *visitor) const override
  {
    visitor->VisitPeriodic(this);
  }

  std::size_t interval{kDefaultIntervalMs};
  std::size_t timeout{kDefaultTimeoutMs};
  std::unique_ptr<PushMetricExporterConfiguration> exporter;
  std::vector<std::unique_ptr<MetricProducerConfiguration>> producers;
  std::unique_ptr<CardinalityLimitsConfiguration> cardinality_limits;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
