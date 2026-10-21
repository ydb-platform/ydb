// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <memory>
#include <utility>

#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter.h"
#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter_factory.h"
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

std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>
OtlpHttpLogRecordExporterFactory::Create()
{
  OtlpHttpLogRecordExporterOptions options;
  return Create(options);
}

std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>
OtlpHttpLogRecordExporterFactory::Create(const OtlpHttpLogRecordExporterOptions &options)
{
  OtlpHttpLogRecordExporterRuntimeOptions runtime_options;
  return Create(options, runtime_options);
}

std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>
OtlpHttpLogRecordExporterFactory::Create(
    const OtlpHttpLogRecordExporterOptions &options,
    const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options)
{
  return std::make_unique<OtlpHttpLogRecordExporter>(options, runtime_options);
}

std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>
OtlpHttpLogRecordExporterFactory::Create(
    const OtlpHttpLogRecordExporterOptions &options,
    const std::shared_ptr<opentelemetry::ext::http::client::HttpClientFactory> &factory)
{
  return std::make_unique<OtlpHttpLogRecordExporter>(options, factory);
}

std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>
OtlpHttpLogRecordExporterFactory::Create(
    const OtlpHttpLogRecordExporterOptions &options,
    const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options,
    const std::shared_ptr<opentelemetry::ext::http::client::HttpClientFactory> &factory)
{
  return std::make_unique<OtlpHttpLogRecordExporter>(options, runtime_options, factory);
}

std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>
OtlpHttpLogRecordExporterFactory::Create(
    const OtlpHttpLogRecordExporterOptions &options,
    std::shared_ptr<opentelemetry::ext::http::client::HttpClient> http_client)
{
  return std::make_unique<OtlpHttpLogRecordExporter>(options, std::move(http_client));
}

std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>
OtlpHttpLogRecordExporterFactory::Create(
    const OtlpHttpLogRecordExporterOptions &options,
    const OtlpHttpLogRecordExporterRuntimeOptions &runtime_options,
    std::shared_ptr<opentelemetry::ext::http::client::HttpClient> http_client)
{
  return std::make_unique<OtlpHttpLogRecordExporter>(options, runtime_options,
                                                     std::move(http_client));
}

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
