// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_runtime_options.h"
#include "opentelemetry/ext/http/client/http_client.h"
#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

/**
 * Factory class for OtlpHttpMetricExporter.
 */
class OPENTELEMETRY_EXPORT OtlpHttpMetricExporterFactory
{
public:
  /**
   * Create a OtlpHttpMetricExporter.
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create();

  /**
   * Create a OtlpHttpMetricExporter.
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      const OtlpHttpMetricExporterOptions &options);

  /**
   * Create a OtlpHttpMetricExporter.
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      const OtlpHttpMetricExporterOptions &options,
      const OtlpHttpMetricExporterRuntimeOptions &runtime_options);

  /**
   * Create a OtlpHttpMetricExporter using the given options and HTTP client factory.
   * @param options the exporter options
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      const OtlpHttpMetricExporterOptions &options,
      const std::shared_ptr<opentelemetry::ext::http::client::HttpClientFactory> &factory);

  /**
   * Create a OtlpHttpMetricExporter using the given options, runtime options, and HTTP client
   * factory.
   * @param options the exporter options
   * @param runtime_options the runtime options (e.g. thread instrumentation)
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      const OtlpHttpMetricExporterOptions &options,
      const OtlpHttpMetricExporterRuntimeOptions &runtime_options,
      const std::shared_ptr<opentelemetry::ext::http::client::HttpClientFactory> &factory);

  /**
   * Create a OtlpHttpMetricExporter using the given options and HTTP client.
   * @param options the exporter options
   * @param http_client the HTTP client to be used for exporting
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      const OtlpHttpMetricExporterOptions &options,
      std::shared_ptr<opentelemetry::ext::http::client::HttpClient> http_client);

  /**
   * Create a OtlpHttpMetricExporter using the given options, runtime options, and HTTP client.
   * @param options the exporter options
   * @param runtime_options the runtime options (e.g. thread instrumentation)
   * @param http_client the HTTP client to be used for exporting
   */
  static std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter> Create(
      const OtlpHttpMetricExporterOptions &options,
      const OtlpHttpMetricExporterRuntimeOptions &runtime_options,
      std::shared_ptr<opentelemetry::ext::http::client::HttpClient> http_client);
};

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
