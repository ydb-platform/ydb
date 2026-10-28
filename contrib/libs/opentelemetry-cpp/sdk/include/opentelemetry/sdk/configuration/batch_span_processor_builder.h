// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/batch_span_processor_configuration.h"
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

class BatchSpanProcessorBuilder
{
public:
  BatchSpanProcessorBuilder()                                                  = default;
  BatchSpanProcessorBuilder(BatchSpanProcessorBuilder &&)                      = default;
  BatchSpanProcessorBuilder(const BatchSpanProcessorBuilder &)                 = default;
  BatchSpanProcessorBuilder &operator=(BatchSpanProcessorBuilder &&)           = default;
  BatchSpanProcessorBuilder &operator=(const BatchSpanProcessorBuilder &other) = default;
  virtual ~BatchSpanProcessorBuilder()                                         = default;

  virtual std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> Build(
      const opentelemetry::sdk::configuration::BatchSpanProcessorConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> &&exporter) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
