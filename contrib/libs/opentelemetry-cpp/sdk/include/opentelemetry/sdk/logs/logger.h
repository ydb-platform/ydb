// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "logger_config.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/logs/log_record.h"
#include "opentelemetry/logs/logger.h"
#include "opentelemetry/logs/noop.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/unique_ptr.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/logs/logger_context.h"
#include "opentelemetry/version.h"

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
#  include "opentelemetry/nostd/variant.h"
#  include "opentelemetry/trace/span_context.h"
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{

class Logger final : public opentelemetry::logs::Logger
{
public:
  /**
   * Initialize a new logger.
   * @param name The name of this logger instance
   * @param context The logger provider that owns this logger.
   * @param instrumentation_scope The instrumentation scope for this logger.
   */
  explicit Logger(
      opentelemetry::nostd::string_view name,
      std::shared_ptr<LoggerContext> context,
      std::unique_ptr<instrumentationscope::InstrumentationScope> instrumentation_scope =
          instrumentationscope::InstrumentationScope::Create("")) noexcept;

  /**
   * Returns the name of this logger.
   */
  const opentelemetry::nostd::string_view GetName() noexcept override;

  nostd::unique_ptr<opentelemetry::logs::LogRecord> CreateLogRecord() noexcept override;

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  nostd::unique_ptr<opentelemetry::logs::LogRecord> CreateLogRecord(
      const nostd::variant<opentelemetry::trace::SpanContext, opentelemetry::context::Context>
          &context_or_span) noexcept override;
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

  using opentelemetry::logs::Logger::EmitLogRecord;

  void EmitLogRecord(
      nostd::unique_ptr<opentelemetry::logs::LogRecord> &&log_record) noexcept override;

  /** Returns the associated instrumentation scope */
  const opentelemetry::sdk::instrumentationscope::InstrumentationScope &GetInstrumentationScope()
      const noexcept;

  OPENTELEMETRY_DEPRECATED_MESSAGE("Please use GetInstrumentationScope instead")
  const opentelemetry::sdk::instrumentationscope::InstrumentationScope &GetInstrumentationLibrary()
      const noexcept
  {
    return GetInstrumentationScope();
  }

protected:
  bool EnabledImplementation(opentelemetry::logs::Severity severity,
                             const opentelemetry::logs::EventId &event_id) const noexcept override;

  bool EnabledImplementation(opentelemetry::logs::Severity severity,
                             int64_t event_id) const noexcept override;

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  bool EnabledImplementation(const nostd::variant<opentelemetry::trace::SpanContext,
                                                  opentelemetry::context::Context> &context_or_span,
                             opentelemetry::logs::Severity severity) const noexcept override;

  bool EnabledImplementation(const nostd::variant<opentelemetry::trace::SpanContext,
                                                  opentelemetry::context::Context> &context_or_span,
                             opentelemetry::logs::Severity severity,
                             const opentelemetry::logs::EventId &event_id) const noexcept override;
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

private:
  // LoggerProvider needs access to UpdateLoggerConfig to propagate configuration updates to
  // existing loggers.
  friend class LoggerProvider;

  /**
   * Update this logger's LoggerConfig. Called only by
   * LoggerProvider::UpdateLoggerConfigurator when the provider-level
   * LoggerConfigurator is replaced at runtime.
   */
  void UpdateLoggerConfig(LoggerConfig config) noexcept;

  // The name of this logger
  std::string logger_name_;

  // order of declaration is important here - instrumentation scope should destroy after
  // logger-context.
  std::unique_ptr<instrumentationscope::InstrumentationScope> instrumentation_scope_;
  std::shared_ptr<LoggerContext> context_;
  // LoggerConfig state is stored in atomic variables to avoid locking in the hot path of Enabled()
  // and EmitLogRecord().
  std::atomic<bool> logger_enabled_{true};
  std::atomic<bool> trace_based_{false};
  static opentelemetry::logs::NoopLogger kNoopLogger;
};

}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
