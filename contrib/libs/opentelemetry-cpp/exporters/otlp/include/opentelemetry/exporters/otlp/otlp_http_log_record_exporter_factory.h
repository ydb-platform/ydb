// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter_runtime_options.h"
#include "opentelemetry/ext/http/client/http_client.h"
#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/sdk/logs/exporter.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

/**
 * Factory class for OtlpHttpLogRecordExporter.
 */
class OPENTELEMETRY_EXPORT OtlpHttpLogRecordExporterFactory
{
public:
  /**
   * Create a OtlpHttpLogRecordExporter.
   */
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> Create();

  /**
   * Create a OtlpHttpLogRecordExporter.
   */
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> Create(
      const OtlpHttpLogRecordExporterOptions &options);

  /**
   * Create a OtlpHttpLogRecordExporter.
   */
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> Create(
      const OtlpHttpLogRecordExporterOptions &options,
      const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options);

  /**
   * Create a OtlpHttpLogRecordExporter using the given options and HTTP client factory.
   * @param options the exporter options
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> Create(
      const OtlpHttpLogRecordExporterOptions &options,
      const std::shared_ptr<opentelemetry::ext::http::client::HttpClientFactory> &factory);

  /**
   * Create a OtlpHttpLogRecordExporter using the given options, runtime options, and HTTP client
   * factory.
   * @param options the exporter options
   * @param runtime_options the runtime options (e.g. thread instrumentation)
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> Create(
      const OtlpHttpLogRecordExporterOptions &options,
      const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options,
      const std::shared_ptr<opentelemetry::ext::http::client::HttpClientFactory> &factory);

  /**
   * Create a OtlpHttpLogRecordExporter using the given options and HTTP client.
   * @param options the exporter options
   * @param http_client the HTTP client to be used for exporting
   */
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> Create(
      const OtlpHttpLogRecordExporterOptions &options,
      std::shared_ptr<opentelemetry::ext::http::client::HttpClient> http_client);

  /**
   * Create a OtlpHttpLogRecordExporter using the given options, runtime options, and HTTP client.
   * @param options the exporter options
   * @param runtime_options the runtime options (e.g. thread instrumentation)
   * @param http_client the HTTP client to be used for exporting
   */
  static std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> Create(
      const OtlpHttpLogRecordExporterOptions &options,
      const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options,
      std::shared_ptr<opentelemetry::ext::http::client::HttpClient> http_client);
};

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
