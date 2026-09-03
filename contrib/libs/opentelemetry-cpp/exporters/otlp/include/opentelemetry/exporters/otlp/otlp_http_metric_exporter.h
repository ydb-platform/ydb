// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>

#include "opentelemetry/exporters/otlp/otlp_http_client.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_runtime_options.h"
#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/sdk/common/exporter_utils.h"
#include "opentelemetry/sdk/metrics/export/metric_producer.h"
#include "opentelemetry/sdk/metrics/instruments.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{
/**
 * The OTLP exporter exports metrics data in OpenTelemetry Protocol (OTLP) format in HTTP.
 */
class OtlpHttpMetricExporter final : public opentelemetry::sdk::metrics::PushMetricExporter
{
public:
  /**
   * Create an OtlpHttpMetricExporter with default exporter options.
   */
  OtlpHttpMetricExporter();

  /**
   * Create an OtlpHttpMetricExporter with user specified options.
   * @param options An object containing the user's configuration options.
   */
  OtlpHttpMetricExporter(const OtlpHttpMetricExporterOptions &options);

  /**
   * Create an OtlpHttpMetricExporter with user specified options.
   * @param options An object containing the user's configuration options.
   * @param runtime_options An object containing the user's runtime options.
   */
  OtlpHttpMetricExporter(const OtlpHttpMetricExporterOptions &options,
                         const OtlpHttpMetricExporterRuntimeOptions &runtime_options);

  /**
   * Create an OtlpHttpMetricExporter with user specified options and HTTP client factory.
   * @param options An object containing the user's configuration options.
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  OtlpHttpMetricExporter(const OtlpHttpMetricExporterOptions &options,
                         const std::shared_ptr<ext::http::client::HttpClientFactory> &factory);

  /**
   * Create an OtlpHttpMetricExporter with user specified options, runtime options, and HTTP client
   * factory.
   * @param options An object containing the user's configuration options.
   * @param runtime_options An object containing the user's runtime options.
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  OtlpHttpMetricExporter(const OtlpHttpMetricExporterOptions &options,
                         const OtlpHttpMetricExporterRuntimeOptions &runtime_options,
                         const std::shared_ptr<ext::http::client::HttpClientFactory> &factory);

  /**
   * Create an OtlpHttpMetricExporter with user specified options and HTTP client.
   * @param options An object containing the user's configuration options.
   * @param http_client the HTTP client to be used for exporting
   */
  OtlpHttpMetricExporter(const OtlpHttpMetricExporterOptions &options,
                         std::shared_ptr<ext::http::client::HttpClient> http_client);

  /**
   * Create an OtlpHttpMetricExporter with user specified options, runtime options, and HTTP client.
   * @param options An object containing the user's configuration options.
   * @param runtime_options An object containing the user's runtime options.
   * @param http_client the HTTP client to be used for exporting
   */
  OtlpHttpMetricExporter(const OtlpHttpMetricExporterOptions &options,
                         const OtlpHttpMetricExporterRuntimeOptions &runtime_options,
                         std::shared_ptr<ext::http::client::HttpClient> http_client);

  /**
   * Get the AggregationTemporality for exporter
   *
   * @return AggregationTemporality
   */
  sdk::metrics::AggregationTemporality GetAggregationTemporality(
      sdk::metrics::InstrumentType instrument_type) const noexcept override;

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::sdk::metrics::ResourceMetrics &data) noexcept override;

  /**
   * Force flush the exporter.
   */
  bool ForceFlush(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

  bool Shutdown(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

private:
  // Configuration options for the exporter
  OtlpHttpMetricExporterOptions options_;
  // Runtime options for the exporter
  OtlpHttpMetricExporterRuntimeOptions runtime_options_;

  // Aggregation Temporality Selector
  sdk::metrics::AggregationTemporalitySelector aggregation_temporality_selector_;

  // Object that stores the HTTP sessions that have been created
  std::unique_ptr<OtlpHttpClient> http_client_;
  // For testing
  friend class OtlpHttpMetricExporterTestPeer;

  /**
   * Create an OtlpHttpMetricExporter using the specified http client.
   * Only tests can call this constructor directly.
   * @param http_client the http client to be used for exporting
   */
  OtlpHttpMetricExporter(std::unique_ptr<OtlpHttpClient> http_client);
};
}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
