// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/configuration/default_histogram_aggregation.h"
#include "opentelemetry/sdk/configuration/headers_configuration.h"
#include "opentelemetry/sdk/configuration/push_metric_exporter_configuration.h"
#include "opentelemetry/sdk/configuration/push_metric_exporter_configuration_visitor.h"
#include "opentelemetry/sdk/configuration/temporality_preference.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/meter_provider.json
// YAML-NODE: ConsoleMetricExporter
class ConsolePushMetricExporterConfiguration : public PushMetricExporterConfiguration
{
public:
  static constexpr TemporalityPreference kDefaultTemporalityPreference =
      TemporalityPreference::cumulative;
  static constexpr DefaultHistogramAggregation kDefaultHistogramAggregation =
      DefaultHistogramAggregation::explicit_bucket_histogram;

  void Accept(PushMetricExporterConfigurationVisitor *visitor) const override
  {
    visitor->VisitConsole(this);
  }

  TemporalityPreference temporality_preference{kDefaultTemporalityPreference};
  DefaultHistogramAggregation default_histogram_aggregation{kDefaultHistogramAggregation};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
