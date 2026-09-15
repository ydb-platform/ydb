// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>

#include "opentelemetry/context/context.h"
#include "opentelemetry/logs/severity.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace instrumentationscope
{
class InstrumentationScope;
}  // namespace instrumentationscope
}  // namespace sdk

namespace sdk
{
namespace logs
{
class Recordable;

/**
 * The Log Processor is responsible for passing log records
 * to the configured exporter.
 */
class LogRecordProcessor
{
public:
  LogRecordProcessor() = default;

  LogRecordProcessor(const LogRecordProcessor &)            = delete;
  LogRecordProcessor(LogRecordProcessor &&)                 = delete;
  LogRecordProcessor &operator=(const LogRecordProcessor &) = delete;
  LogRecordProcessor &operator=(LogRecordProcessor &&)      = delete;

  virtual ~LogRecordProcessor() = default;

  /**
   * Create a log recordable. This requests a new log recordable from the
   * associated exporter.
   *
   * @return a newly initialized recordable
   *
   * Note: This method must be callable from multiple threads.
   */
  virtual std::unique_ptr<Recordable> MakeRecordable() noexcept = 0;

  /**
   * OnEmit is called by the SDK once a log record has been successfully created.
   * @param record the log recordable object
   */
  virtual void OnEmit(std::unique_ptr<Recordable> &&record) noexcept = 0;

  /**
   * Enabled returns whether this processor is interested in a log with the given inputs.
   * The default implementation is permissive and returns true.
   */
  bool Enabled(
      const opentelemetry::nostd::variant<opentelemetry::trace::SpanContext,
                                          opentelemetry::context::Context> &context_or_span,
      const opentelemetry::sdk::instrumentationscope::InstrumentationScope &instrumentation_scope,
      opentelemetry::logs::Severity severity,
      opentelemetry::nostd::string_view event_name = {}) const noexcept
  {
    return EnabledImplementation(context_or_span, instrumentation_scope, severity, event_name);
  }

  /**
   * Exports all log records that have not yet been exported to the configured Exporter.
   * @param timeout that the forceflush is required to finish within.
   * @return a result code indicating whether it succeeded, failed or timed out
   */
  virtual bool ForceFlush(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept = 0;

  /**
   * Shuts down the processor and does any cleanup required.
   * ShutDown should only be called once for each processor.
   * @param timeout minimum amount of microseconds to wait for
   * shutdown before giving up and returning failure.
   * @return true if the shutdown succeeded, false otherwise
   */
  virtual bool Shutdown(
      std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) noexcept = 0;

  /**
   * Returns true if this processor's EnabledImplementation does any custom
   * filtering. The default returns true (conservative — assume any subclass
   * might filter), so the SDK Logger consults the full Enabled chain by
   * default.
   */
  virtual bool HasEnabledFilter() const noexcept { return true; }

protected:
  virtual bool EnabledImplementation(
      const opentelemetry::nostd::variant<opentelemetry::trace::SpanContext,
                                          opentelemetry::context::Context> & /*context_or_span*/,
      const opentelemetry::sdk::instrumentationscope::InstrumentationScope
          & /*instrumentation_scope*/,
      opentelemetry::logs::Severity /*severity*/,
      opentelemetry::nostd::string_view /*event_name*/) const noexcept
  {
    return true;
  }

public:
  /**
   * Returns true when records produced through this processor enforce LogRecord
   * attribute limits (count and value length). The default returns false.
   * Processors backed by an exporter delegate to the exporter; composite
   * processors return true if any child does. The SDK Logger consults this once
   * per context to decide whether to push limits onto each recordable.
   */
  virtual bool RecordableEnforcesLogRecordLimits() const noexcept { return false; }
};
}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
