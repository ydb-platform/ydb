// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>
#include <vector>

#include "logger_config.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/logs/log_record_limits.h"
#include "opentelemetry/sdk/logs/processor.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{

/**
 * A class which stores the LoggerContext context.
 *
 * This class meets the following design criteria:
 * - A shared reference between LoggerProvider and Logger instantiated.
 * - A thread-safe class that allows updating/altering processor/exporter pipelines
 *   and sampling config.
 * - The owner/destroyer of Processors/Exporters.  These will remain active until
 *   this class is destroyed.  I.e. Sampling, Exporting, flushing, Custom Iterator etc. are all ok
 * if this object is alive, and they will work together. If this object is destroyed, then no shared
 * references to Processor, Exporter, Recordable, Custom Iterator etc. should exist, and all
 *   associated pipelines will have been flushed.
 */
class LoggerContext
{
public:
  explicit LoggerContext(
      std::vector<std::unique_ptr<LogRecordProcessor>> &&processors,
      const opentelemetry::sdk::resource::Resource &resource =
          opentelemetry::sdk::resource::Resource::Create({}),
      std::unique_ptr<instrumentationscope::ScopeConfigurator<LoggerConfig>> logger_configurator =
          std::make_unique<instrumentationscope::ScopeConfigurator<LoggerConfig>>(
              instrumentationscope::ScopeConfigurator<LoggerConfig>::Builder(
                  LoggerConfig::Default())
                  .Build()),
      LogRecordLimits log_record_limits = LogRecordLimits()) noexcept;

  /**
   * Attaches a log processor to list of configured processors to this logger context.
   * Processor once attached can't be removed.
   * @param processor The new log processor for this tracer. This must not be
   * a nullptr. Ownership is given to the `LoggerContext`.
   *
   * Note: This method is not thread safe.
   */
  void AddProcessor(std::unique_ptr<LogRecordProcessor> processor) noexcept;

  /**
   * Obtain the configured (composite) processor.
   *
   * Note: When more than one processor is active, this will
   * return an "aggregate" processor
   */
  LogRecordProcessor &GetProcessor() const noexcept;

  /**
   * Obtain the resource associated with this tracer context.
   * @return The resource for this tracer context.
   */
  const opentelemetry::sdk::resource::Resource &GetResource() const noexcept;

  /**
   * Obtain the ScopeConfigurator with this logger context.
   * @return The ScopeConfigurator for this logger context.
   */
  const instrumentationscope::ScopeConfigurator<LoggerConfig> &GetLoggerConfigurator()
      const noexcept;

  /**
   * Obtain the LogRecord limits applied by this context.
   * @return The LogRecordLimits for this logger context.
   */
  const LogRecordLimits &GetLogRecordLimits() const noexcept;

  /**
   * Whether any configured processor produces a recordable that enforces the
   * LogRecord attribute limits. Computed once at construction (and refreshed by
   * AddProcessor) so the Logger hot path can skip the per-record
   * SetLogRecordLimits() virtual call when no processor needs it.
   * @return true if at least one processor enforces limits.
   */
  bool RecordableEnforcesLogRecordLimits() const noexcept;

  /**
   * Replace the ScopeConfigurator for this logger context.
   * @param logger_configurator The new configurator.
   */
  void SetLoggerConfigurator(std::unique_ptr<instrumentationscope::ScopeConfigurator<LoggerConfig>>
                                 logger_configurator) noexcept;

  /**
   * Force all active LogProcessors to flush any buffered logs
   * within the given timeout.
   */
  bool ForceFlush(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept;

  /**
   * Shutdown the log processor associated with this tracer provider.
   */
  bool Shutdown(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept;

private:
  //  order of declaration is important here - resource object should be destroyed after processor.
  opentelemetry::sdk::resource::Resource resource_;
  std::unique_ptr<LogRecordProcessor> processor_;

  std::unique_ptr<instrumentationscope::ScopeConfigurator<LoggerConfig>> logger_configurator_;

  LogRecordLimits log_record_limits_;

  // Cached capability flag: true when at least one configured processor's
  // recordable enforces the LogRecord attribute limits. Refreshed by
  // AddProcessor so the Logger hot path reads a plain bool.
  bool recordable_enforces_limits_{false};
};
}  // namespace logs
}  // namespace sdk

OPENTELEMETRY_END_NAMESPACE
