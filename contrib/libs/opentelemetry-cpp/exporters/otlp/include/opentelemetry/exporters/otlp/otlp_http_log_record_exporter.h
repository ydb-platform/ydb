// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>

#include "opentelemetry/exporters/otlp/otlp_http_client.h"
#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter_runtime_options.h"
#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/sdk/common/exporter_utils.h"
#include "opentelemetry/sdk/logs/exporter.h"
#include "opentelemetry/sdk/logs/recordable.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

/**
 * The OTLP exporter exports log data in OpenTelemetry Protocol (OTLP) format.
 */
class OtlpHttpLogRecordExporter final : public opentelemetry::sdk::logs::LogRecordExporter
{
public:
  /**
   * Create an OtlpHttpLogRecordExporter with default exporter options.
   */
  OtlpHttpLogRecordExporter();

  /**
   * Create an OtlpHttpLogRecordExporter with user specified options.
   * @param options An object containing the user's configuration options.
   */
  OtlpHttpLogRecordExporter(const OtlpHttpLogRecordExporterOptions &options);

  /**
   * Create an OtlpHttpLogRecordExporter with user specified options.
   * @param options An object containing the user's configuration options.
   * @param runtime_options An object containing the user's runtime options.
   */
  OtlpHttpLogRecordExporter(const OtlpHttpLogRecordExporterOptions &options,
                            const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options);

  /**
   * Create an OtlpHttpLogRecordExporter with user specified options and HTTP client factory.
   * @param options An object containing the user's configuration options.
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  OtlpHttpLogRecordExporter(const OtlpHttpLogRecordExporterOptions &options,
                            const std::shared_ptr<ext::http::client::HttpClientFactory> &factory);

  /**
   * Create an OtlpHttpLogRecordExporter with user specified options, runtime options, and HTTP
   * client factory.
   * @param options An object containing the user's configuration options.
   * @param runtime_options An object containing the user's runtime options.
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  OtlpHttpLogRecordExporter(const OtlpHttpLogRecordExporterOptions &options,
                            const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options,
                            const std::shared_ptr<ext::http::client::HttpClientFactory> &factory);

  /**
   * Create an OtlpHttpLogRecordExporter with user specified options and HTTP client.
   * @param options An object containing the user's configuration options.
   * @param http_client the HTTP client to be used for exporting
   */
  OtlpHttpLogRecordExporter(const OtlpHttpLogRecordExporterOptions &options,
                            std::shared_ptr<ext::http::client::HttpClient> http_client);

  /**
   * Create an OtlpHttpLogRecordExporter with user specified options, runtime options, and HTTP
   * client.
   * @param options An object containing the user's configuration options.
   * @param runtime_options An object containing the user's runtime options.
   * @param http_client the HTTP client to be used for exporting
   */
  OtlpHttpLogRecordExporter(const OtlpHttpLogRecordExporterOptions &options,
                            const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options,
                            std::shared_ptr<ext::http::client::HttpClient> http_client);

  /**
   * Creates a recordable that stores the data in a JSON object
   */
  std::unique_ptr<opentelemetry::sdk::logs::Recordable> MakeRecordable() noexcept override;

  /**
   * The OTLP recordable applies the configured LogRecord attribute limits, so
   * the SDK should push the limits onto each record it produces.
   */
  bool RecordableEnforcesLogRecordLimits() const noexcept override { return true; }

  /**
   * Exports a vector of log records to the Elasticsearch instance. Guaranteed to return after a
   * timeout specified from the options passed from the constructor.
   * @param records A list of log records to send to Elasticsearch.
   */
  opentelemetry::sdk::common::ExportResult Export(
      const nostd::span<std::unique_ptr<opentelemetry::sdk::logs::Recordable>> &records) noexcept
      override;

  /**
   * Force flush the exporter.
   * @param timeout an option timeout, default to max.
   * @return return true when all data are exported, and false when timeout
   */
  bool ForceFlush(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

  /**
   * Shutdown this exporter.
   * @param timeout The maximum time to wait for the shutdown method to return
   */
  bool Shutdown(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

private:
  // Configuration options for the exporter
  OtlpHttpLogRecordExporterOptions options_;
  // Runtime options for the exporter
  OtlpHttpLogRecordExporterRuntimeOptions runtime_options_;

  // Object that stores the HTTP sessions that have been created
  std::unique_ptr<OtlpHttpClient> http_client_;
  // For testing
  friend class OtlpHttpLogRecordExporterTestPeer;
  /**
   * Create an OtlpHttpLogRecordExporter using the specified http client.
   * Only tests can call this constructor directly.
   * @param http_client the http client to be used for exporting
   */
  OtlpHttpLogRecordExporter(std::unique_ptr<OtlpHttpClient> http_client);
};
}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
