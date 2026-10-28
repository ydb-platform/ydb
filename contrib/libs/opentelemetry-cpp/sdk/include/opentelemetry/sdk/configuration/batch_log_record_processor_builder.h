// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/batch_log_record_processor_configuration.h"
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

class BatchLogRecordProcessorBuilder
{
public:
  BatchLogRecordProcessorBuilder()                                                       = default;
  BatchLogRecordProcessorBuilder(BatchLogRecordProcessorBuilder &&)                      = default;
  BatchLogRecordProcessorBuilder(const BatchLogRecordProcessorBuilder &)                 = default;
  BatchLogRecordProcessorBuilder &operator=(BatchLogRecordProcessorBuilder &&)           = default;
  BatchLogRecordProcessorBuilder &operator=(const BatchLogRecordProcessorBuilder &other) = default;
  virtual ~BatchLogRecordProcessorBuilder()                                              = default;

  virtual std::unique_ptr<opentelemetry::sdk::logs::LogRecordProcessor> Build(
      const opentelemetry::sdk::configuration::BatchLogRecordProcessorConfiguration *model,
      std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter> &&exporter) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
