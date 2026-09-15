// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// MUST be first (absl)
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter.h"

#include <memory>

#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_options.h"
#include "opentelemetry/sdk/metrics/push_metric_exporter.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter>
OtlpGrpcMetricExporterFactory::Create()
{
  OtlpGrpcMetricExporterOptions options;
  return Create(options);
}

std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter>
OtlpGrpcMetricExporterFactory::Create(const OtlpGrpcMetricExporterOptions &options)
{
  return std::make_unique<OtlpGrpcMetricExporter>(options);
}

std::unique_ptr<opentelemetry::sdk::metrics::PushMetricExporter>
OtlpGrpcMetricExporterFactory::Create(const OtlpGrpcMetricExporterOptions &options,
                                      const std::shared_ptr<OtlpGrpcClient> &client)
{
  return std::make_unique<OtlpGrpcMetricExporter>(options, client);
}

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
