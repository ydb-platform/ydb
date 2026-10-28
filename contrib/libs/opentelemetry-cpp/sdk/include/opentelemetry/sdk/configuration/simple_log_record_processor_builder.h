// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/simple_log_record_processor_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{
class LogRecordExporter;
class LogRecordProcessor;
}  // namespace logs

namespace configuration
{

class SimpleLogRecordProcessorBuilder
{
public:
  SimpleLogRecordProcessorBuilder()                                              = default;
  SimpleLogRecordProcessorBuilder(SimpleLogRecordProcessorBuilder &&)            = default;
  SimpleLogRecordProcessorBuilder(const SimpleLogRecordProcessorBuilder &)       = default;
  SimpleLogRecordProcessorBuilder &operator=(SimpleLogRecordProcessorBuilder &&) = default;
  SimpleLogRecordProcessorBuilder &operator=(const SimpleLogRecordProcessorBuilder &other) =
      default;
  virtual ~SimpleLogRecordProcessorBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::logs::LogRecordProcessor> Build(
      const opentelemetry::sdk::configuration::SimpleLogRecordProcessorConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> &&exporter) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
