// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <memory>

#include "opentelemetry/sdk/configuration/log_record_exporter_configuration.h"
#include "opentelemetry/sdk/configuration/log_record_processor_configuration.h"
#include "opentelemetry/sdk/configuration/log_record_processor_configuration_visitor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/logger_provider.json
// YAML-NODE: BatchLogRecordProcessor
class BatchLogRecordProcessorConfiguration : public LogRecordProcessorConfiguration
{
public:
  // TODO: spec default is 1000ms for schedule_delay, using 5000ms to preserve original behavior
  static constexpr std::size_t kDefaultScheduleDelayMs    = 5000;
  static constexpr std::size_t kDefaultExportTimeoutMs    = 30000;
  static constexpr std::size_t kDefaultMaxQueueSize       = 2048;
  static constexpr std::size_t kDefaultMaxExportBatchSize = 512;

  void Accept(LogRecordProcessorConfigurationVisitor *visitor) const override
  {
    visitor->VisitBatch(this);
  }

  std::size_t schedule_delay{kDefaultScheduleDelayMs};
  std::size_t export_timeout{kDefaultExportTimeoutMs};
  std::size_t max_queue_size{kDefaultMaxQueueSize};
  std::size_t max_export_batch_size{kDefaultMaxExportBatchSize};
  std::unique_ptr<LogRecordExporterConfiguration> exporter;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
