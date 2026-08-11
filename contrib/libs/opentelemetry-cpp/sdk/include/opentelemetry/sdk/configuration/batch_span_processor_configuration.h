// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <memory>

#include "opentelemetry/sdk/configuration/span_exporter_configuration.h"
#include "opentelemetry/sdk/configuration/span_processor_configuration.h"
#include "opentelemetry/sdk/configuration/span_processor_configuration_visitor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/tracer_provider.json
// YAML-NODE: BatchSpanProcessor
class BatchSpanProcessorConfiguration : public SpanProcessorConfiguration
{
public:
  static constexpr std::size_t kDefaultScheduleDelayMs    = 5000;
  static constexpr std::size_t kDefaultExportTimeoutMs    = 30000;
  static constexpr std::size_t kDefaultMaxQueueSize       = 2048;
  static constexpr std::size_t kDefaultMaxExportBatchSize = 512;

  void Accept(SpanProcessorConfigurationVisitor *visitor) const override
  {
    visitor->VisitBatch(this);
  }

  std::size_t schedule_delay{kDefaultScheduleDelayMs};
  std::size_t export_timeout{kDefaultExportTimeoutMs};
  std::size_t max_queue_size{kDefaultMaxQueueSize};
  std::size_t max_export_batch_size{kDefaultMaxExportBatchSize};
  std::unique_ptr<SpanExporterConfiguration> exporter;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
