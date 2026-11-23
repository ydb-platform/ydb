// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/extension_span_exporter_configuration.h"
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

class ExtensionSpanExporterBuilder
{
public:
  ExtensionSpanExporterBuilder()                                                     = default;
  ExtensionSpanExporterBuilder(ExtensionSpanExporterBuilder &&)                      = default;
  ExtensionSpanExporterBuilder(const ExtensionSpanExporterBuilder &)                 = default;
  ExtensionSpanExporterBuilder &operator=(ExtensionSpanExporterBuilder &&)           = default;
  ExtensionSpanExporterBuilder &operator=(const ExtensionSpanExporterBuilder &other) = default;
  virtual ~ExtensionSpanExporterBuilder()                                            = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> Build(
      const opentelemetry::sdk::configuration::ExtensionSpanExporterConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
