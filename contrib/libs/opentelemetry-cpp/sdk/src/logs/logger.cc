// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <stdint.h>
#include <atomic>
#include <chrono>
#include <string>
#include <utility>

#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/context/context_value.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/logs/event_id.h"
#include "opentelemetry/logs/log_record.h"
#include "opentelemetry/logs/noop.h"
#include "opentelemetry/logs/severity.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/unique_ptr.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/instrumentationscope/instrumentation_scope.h"
#include "opentelemetry/sdk/instrumentationscope/scope_configurator.h"
#include "opentelemetry/sdk/logs/logger.h"
#include "opentelemetry/sdk/logs/logger_config.h"
#include "opentelemetry/sdk/logs/logger_context.h"
#include "opentelemetry/sdk/logs/processor.h"
#include "opentelemetry/sdk/logs/recordable.h"
#include "opentelemetry/trace/span.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_id.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/trace_flags.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace logs
{
namespace trace_api = opentelemetry::trace;
namespace context   = opentelemetry::context;
namespace nostd     = opentelemetry::nostd;

namespace
{
nostd::string_view GetEventName(const opentelemetry::logs::EventId &event_id) noexcept
{
  return event_id.name_ != nullptr ? nostd::string_view{event_id.name_.get()}
                                   : nostd::string_view{};
}

trace_api::SpanContext ExtractSpanContextFromContext(const context::Context &context) noexcept
{
  if (!context.HasKey(trace_api::kSpanKey))
  {
    return trace_api::SpanContext::GetInvalid();
  }

  const context::ContextValue context_value = context.GetValue(trace_api::kSpanKey);

  // Get the span metadata from the active span in the context
  if (const nostd::shared_ptr<trace_api::Span> *maybe_span =
          nostd::get_if<nostd::shared_ptr<trace_api::Span>>(&context_value))
  {
    const nostd::shared_ptr<trace_api::Span> &span = *maybe_span;
    return span->GetContext();
  }
  // Get the span metadata directly from a SpanContext in the context.
  // TODO: This path is unused and may be removed in the future.
  if (const nostd::shared_ptr<trace_api::SpanContext> *maybe_span_context =
          nostd::get_if<nostd::shared_ptr<trace_api::SpanContext>>(&context_value))
  {
    const nostd::shared_ptr<trace_api::SpanContext> &span_context = *maybe_span_context;
    return *span_context;
  }
  return trace_api::SpanContext::GetInvalid();
}

trace_api::SpanContext ExtractSpanContext(
    const nostd::variant<trace_api::SpanContext, context::Context> &context_or_span) noexcept
{
  if (const trace_api::SpanContext *sc = nostd::get_if<trace_api::SpanContext>(&context_or_span))
  {
    return *sc;
  }
  if (const context::Context *ctx = nostd::get_if<context::Context>(&context_or_span))
  {
    return ExtractSpanContextFromContext(*ctx);
  }
  return trace_api::SpanContext::GetInvalid();
}

bool IsAllowedByTraceBasedFiltering(
    const nostd::variant<trace_api::SpanContext, context::Context> &context_or_span,
    bool trace_based) noexcept
{
  if (!trace_based)
  {
    return true;
  }

  const trace_api::SpanContext span_context = ExtractSpanContext(context_or_span);

  if (!span_context.span_id().IsValid())
  {
    return true;
  }

  return span_context.trace_flags().IsSampled();
}

void StampSpanContextFromVariant(
    const nostd::variant<trace_api::SpanContext, context::Context> &context_or_span,
    Recordable &recordable) noexcept
{
  const trace_api::SpanContext span_context = ExtractSpanContext(context_or_span);
  if (span_context.IsValid())
  {
    recordable.SetTraceId(span_context.trace_id());
    recordable.SetTraceFlags(span_context.trace_flags());
    recordable.SetSpanId(span_context.span_id());
  }
}
}  // namespace

opentelemetry::logs::NoopLogger Logger::kNoopLogger = opentelemetry::logs::NoopLogger();

Logger::Logger(
    opentelemetry::nostd::string_view name,
    std::shared_ptr<LoggerContext> context,
    std::unique_ptr<instrumentationscope::InstrumentationScope> instrumentation_scope) noexcept
    : logger_name_(std::string(name)),
      instrumentation_scope_(std::move(instrumentation_scope)),
      context_(std::move(context))
{
  LoggerConfig config = context_->GetLoggerConfigurator().ComputeConfig(*instrumentation_scope_);
  UpdateLoggerConfig(config);
}

void Logger::UpdateLoggerConfig(LoggerConfig config) noexcept
{
  logger_enabled_.store(config.IsEnabled(), std::memory_order_relaxed);
  trace_based_.store(config.IsTraceBased(), std::memory_order_relaxed);

  SetMinimumSeverity(config.IsEnabled() ? static_cast<uint8_t>(config.GetMinimumSeverity())
                                        : opentelemetry::logs::kMaxSeverity);

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  SetExtendedEnabledRequired(config.IsTraceBased() || context_->GetProcessor().HasEnabledFilter());
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2
}

const opentelemetry::nostd::string_view Logger::GetName() noexcept
{
  if (!logger_enabled_.load(std::memory_order_relaxed))
  {
    return kNoopLogger.GetName();
  }
  return logger_name_;
}

opentelemetry::nostd::unique_ptr<opentelemetry::logs::LogRecord> Logger::CreateLogRecord() noexcept
{
  if (!logger_enabled_.load(std::memory_order_relaxed))
  {
    return kNoopLogger.CreateLogRecord();
  }

  auto recordable = context_->GetProcessor().MakeRecordable();

  if (context_->RecordableEnforcesLogRecordLimits())
  {
    recordable->SetLogRecordLimits(context_->GetLogRecordLimits());
  }
  recordable->SetObservedTimestamp(std::chrono::system_clock::now());

  StampSpanContextFromVariant(
      nostd::variant<trace_api::SpanContext, context::Context>{
          context::RuntimeContext::GetCurrent()},
      *recordable);

  return opentelemetry::nostd::unique_ptr<opentelemetry::logs::LogRecord>(recordable.release());
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
opentelemetry::nostd::unique_ptr<opentelemetry::logs::LogRecord> Logger::CreateLogRecord(
    const nostd::variant<trace_api::SpanContext, opentelemetry::context::Context>
        &context_or_span) noexcept
{
  if (!logger_enabled_.load(std::memory_order_relaxed))
  {
    return kNoopLogger.CreateLogRecord();
  }

  auto recordable = context_->GetProcessor().MakeRecordable();

  if (context_->RecordableEnforcesLogRecordLimits())
  {
    recordable->SetLogRecordLimits(context_->GetLogRecordLimits());
  }
  recordable->SetObservedTimestamp(std::chrono::system_clock::now());

  StampSpanContextFromVariant(context_or_span, *recordable);

  return opentelemetry::nostd::unique_ptr<opentelemetry::logs::LogRecord>(recordable.release());
}
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

void Logger::EmitLogRecord(
    opentelemetry::nostd::unique_ptr<opentelemetry::logs::LogRecord> &&log_record) noexcept
{
  if (!logger_enabled_.load(std::memory_order_relaxed))
  {
    return kNoopLogger.EmitLogRecord(std::move(log_record));
  }

  if (!log_record)
  {
    return;
  }

  std::unique_ptr<Recordable> recordable =
      std::unique_ptr<Recordable>(static_cast<Recordable *>(log_record.release()));
  recordable->SetResource(context_->GetResource());
  recordable->SetInstrumentationScope(GetInstrumentationScope());

  auto &processor = context_->GetProcessor();

  // Send the log recordable to the processor
  processor.OnEmit(std::move(recordable));
}

bool Logger::EnabledImplementation(opentelemetry::logs::Severity severity,
                                   const opentelemetry::logs::EventId &event_id) const noexcept
{
  const nostd::variant<trace_api::SpanContext, context::Context> current{
      context::RuntimeContext::GetCurrent()};
  if (!IsAllowedByTraceBasedFiltering(current, trace_based_.load(std::memory_order_relaxed)))
  {
    return false;
  }

  return context_->GetProcessor().Enabled(current, GetInstrumentationScope(), severity,
                                          GetEventName(event_id));
}

bool Logger::EnabledImplementation(opentelemetry::logs::Severity severity,
                                   int64_t /*event_id*/) const noexcept
{
  const nostd::variant<trace_api::SpanContext, context::Context> current{
      context::RuntimeContext::GetCurrent()};
  if (!IsAllowedByTraceBasedFiltering(current, trace_based_.load(std::memory_order_relaxed)))
  {
    return false;
  }

  return context_->GetProcessor().Enabled(current, GetInstrumentationScope(), severity);
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
bool Logger::EnabledImplementation(
    const nostd::variant<trace_api::SpanContext, opentelemetry::context::Context> &context_or_span,
    opentelemetry::logs::Severity severity) const noexcept
{
  if (!IsAllowedByTraceBasedFiltering(context_or_span,
                                      trace_based_.load(std::memory_order_relaxed)))
  {
    return false;
  }

  return context_->GetProcessor().Enabled(context_or_span, GetInstrumentationScope(), severity);
}

bool Logger::EnabledImplementation(
    const nostd::variant<trace_api::SpanContext, opentelemetry::context::Context> &context_or_span,
    opentelemetry::logs::Severity severity,
    const opentelemetry::logs::EventId &event_id) const noexcept
{
  if (!IsAllowedByTraceBasedFiltering(context_or_span,
                                      trace_based_.load(std::memory_order_relaxed)))
  {
    return false;
  }

  return context_->GetProcessor().Enabled(context_or_span, GetInstrumentationScope(), severity,
                                          GetEventName(event_id));
}
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

const opentelemetry::sdk::instrumentationscope::InstrumentationScope &
Logger::GetInstrumentationScope() const noexcept
{
  return *instrumentation_scope_;
}

}  // namespace logs
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
