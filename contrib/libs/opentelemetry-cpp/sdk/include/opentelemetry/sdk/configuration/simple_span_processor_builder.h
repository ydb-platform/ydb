// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/simple_span_processor_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
class SpanExporter;
class SpanProcessor;
}  // namespace trace

namespace configuration
{

class SimpleSpanProcessorBuilder
{
public:
  SimpleSpanProcessorBuilder()                                                   = default;
  SimpleSpanProcessorBuilder(SimpleSpanProcessorBuilder &&)                      = default;
  SimpleSpanProcessorBuilder(const SimpleSpanProcessorBuilder &)                 = default;
  SimpleSpanProcessorBuilder &operator=(SimpleSpanProcessorBuilder &&)           = default;
  SimpleSpanProcessorBuilder &operator=(const SimpleSpanProcessorBuilder &other) = default;
  virtual ~SimpleSpanProcessorBuilder()                                          = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> Build(
      const opentelemetry::sdk::configuration::SimpleSpanProcessorConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> &&exporter) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
