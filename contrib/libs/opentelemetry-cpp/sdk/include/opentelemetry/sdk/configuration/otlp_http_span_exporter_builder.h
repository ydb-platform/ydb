// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/otlp_http_span_exporter_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
class SpanExporter;
}  // namespace trace

namespace configuration
{

class OtlpHttpSpanExporterBuilder
{
public:
  OtlpHttpSpanExporterBuilder()                                                    = default;
  OtlpHttpSpanExporterBuilder(OtlpHttpSpanExporterBuilder &&)                      = default;
  OtlpHttpSpanExporterBuilder(const OtlpHttpSpanExporterBuilder &)                 = default;
  OtlpHttpSpanExporterBuilder &operator=(OtlpHttpSpanExporterBuilder &&)           = default;
  OtlpHttpSpanExporterBuilder &operator=(const OtlpHttpSpanExporterBuilder &other) = default;
  virtual ~OtlpHttpSpanExporterBuilder()                                           = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> Build(
      const opentelemetry::sdk::configuration::OtlpHttpSpanExporterConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
