// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>

#include "opentelemetry/exporters/otlp/otlp_http_client.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_runtime_options.h"
#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/sdk/common/exporter_utils.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/recordable.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

/**
 * The OTLP exporter exports span data in OpenTelemetry Protocol (OTLP) format.
 */
class OPENTELEMETRY_EXPORT OtlpHttpExporter final : public opentelemetry::sdk::trace::SpanExporter
{
public:
  /**
   * Create an OtlpHttpExporter using all default options.
   */
  OtlpHttpExporter();

  /**
   * Create an OtlpHttpExporter using the given options.
   */
  explicit OtlpHttpExporter(const OtlpHttpExporterOptions &options);

  /**
   * Create an OtlpHttpExporter using the given options.
   */
  OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                   const OtlpHttpExporterRuntimeOptions &runtime_options);

  /**
   * Create an OtlpHttpExporter using the given options and HTTP client factory.
   * @param options the exporter options
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                   const std::shared_ptr<ext::http::client::HttpClientFactory> &factory);

  /**
   * Create an OtlpHttpExporter using the given options, runtime options, and HTTP client factory.
   * @param options the exporter options
   * @param runtime_options the runtime options (e.g. thread instrumentation)
   * @param factory the HTTP client factory used to create the underlying HTTP client
   */
  OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                   const OtlpHttpExporterRuntimeOptions &runtime_options,
                   const std::shared_ptr<ext::http::client::HttpClientFactory> &factory);

  /**
   * Create an OtlpHttpExporter using the given options and HTTP client.
   * @param options the exporter options
   * @param http_client the HTTP client to be used for exporting
   */
  OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                   std::shared_ptr<ext::http::client::HttpClient> http_client);

  /**
   * Create an OtlpHttpExporter using the given options, runtime options, and HTTP client.
   * @param options the exporter options
   * @param runtime_options the runtime options (e.g. thread instrumentation)
   * @param http_client the HTTP client to be used for exporting
   */
  OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                   const OtlpHttpExporterRuntimeOptions &runtime_options,
                   std::shared_ptr<ext::http::client::HttpClient> http_client);

  /**
   * Create a span recordable.
   * @return a newly initialized Recordable object
   */
  std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable() noexcept override;

  /**
   * Export
   * @param spans a span of unique pointers to span recordables
   */
  opentelemetry::sdk::common::ExportResult Export(
      const nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>> &spans) noexcept
      override;

  /**
   * Force flush the exporter.
   * @param timeout an option timeout, default to max.
   * @return return true when all data are exported, and false when timeout
   */
  bool ForceFlush(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

  /**
   * Shut down the exporter.
   * @param timeout an optional timeout, the default timeout of 0 means that no
   * timeout is applied.
   * @return return the status of this operation
   */
  bool Shutdown(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept override;

private:
  // The configuration options associated with this exporter.
  OtlpHttpExporterOptions options_;
  // The runtime options associated with this exporter.
  OtlpHttpExporterRuntimeOptions runtime_options_;

  // Object that stores the HTTP sessions that have been created
  std::unique_ptr<OtlpHttpClient> http_client_;
  // For testing
  friend class OtlpHttpExporterTestPeer;
  friend class OtlpHttpExporterCustomClientTestPeer;
  /**
   * Create an OtlpHttpExporter using the specified http client.
   * Only tests can call this constructor directly.
   * @param http_client the http client to be used for exporting
   */
  OtlpHttpExporter(std::unique_ptr<OtlpHttpClient> http_client);
};
}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
