// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Make sure to include GRPC exporter first because otherwise Abseil may create
// ambiguity with `nostd::variant`. See issue:
// https://github.com/open-telemetry/opentelemetry-cpp/issues/880
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter.h"

#include <memory>

#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> OtlpGrpcExporterFactory::Create()
{
  OtlpGrpcExporterOptions options;
  return Create(options);
}

std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> OtlpGrpcExporterFactory::Create(
    const OtlpGrpcExporterOptions &options)
{
  return std::make_unique<OtlpGrpcExporter>(options);
}

std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> OtlpGrpcExporterFactory::Create(
    const OtlpGrpcExporterOptions &options,
    const std::shared_ptr<OtlpGrpcClient> &client)
{
  return std::make_unique<OtlpGrpcExporter>(options, client);
}

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
