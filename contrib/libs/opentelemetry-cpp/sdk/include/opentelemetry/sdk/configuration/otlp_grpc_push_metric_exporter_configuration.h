// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "opentelemetry/sdk/configuration/default_histogram_aggregation.h"
#include "opentelemetry/sdk/configuration/grpc_tls_configuration.h"
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
// YAML-NODE: OtlpGrpcMetricExporter
class OtlpGrpcPushMetricExporterConfiguration : public PushMetricExporterConfiguration
{
public:
  static constexpr std::size_t kDefaultTimeoutMs = 10000;
  static constexpr TemporalityPreference kDefaultTemporalityPreference =
      TemporalityPreference::cumulative;
  static constexpr DefaultHistogramAggregation kDefaultHistogramAggregation =
      DefaultHistogramAggregation::explicit_bucket_histogram;

  void Accept(PushMetricExporterConfigurationVisitor *visitor) const override
  {
    visitor->VisitOtlpGrpc(this);
  }

  std::string endpoint;
  std::unique_ptr<GrpcTlsConfiguration> tls;
  std::unique_ptr<HeadersConfiguration> headers;
  std::string headers_list;
  std::string compression;
  std::size_t timeout{kDefaultTimeoutMs};
  TemporalityPreference temporality_preference{kDefaultTemporalityPreference};
  DefaultHistogramAggregation default_histogram_aggregation{kDefaultHistogramAggregation};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
