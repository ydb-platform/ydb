// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "opentelemetry/sdk/configuration/default_histogram_aggregation.h"
#include "opentelemetry/sdk/configuration/headers_configuration.h"
#include "opentelemetry/sdk/configuration/http_tls_configuration.h"
#include "opentelemetry/sdk/configuration/otlp_http_encoding.h"
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
// YAML-NODE: OtlpHttpMetricExporter
class OtlpHttpPushMetricExporterConfiguration : public PushMetricExporterConfiguration
{
public:
  static constexpr std::size_t kDefaultTimeoutMs = 10000;
  static constexpr TemporalityPreference kDefaultTemporalityPreference =
      TemporalityPreference::cumulative;
  static constexpr DefaultHistogramAggregation kDefaultHistogramAggregation =
      DefaultHistogramAggregation::explicit_bucket_histogram;

  void Accept(PushMetricExporterConfigurationVisitor *visitor) const override
  {
    visitor->VisitOtlpHttp(this);
  }

  std::string endpoint;
  std::unique_ptr<HttpTlsConfiguration> tls;
  std::unique_ptr<HeadersConfiguration> headers;
  std::string headers_list;
  std::string compression;
  std::size_t timeout{kDefaultTimeoutMs};
  OtlpHttpEncoding encoding{OtlpHttpEncoding::protobuf};
  TemporalityPreference temporality_preference{kDefaultTemporalityPreference};
  DefaultHistogramAggregation default_histogram_aggregation{kDefaultHistogramAggregation};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
