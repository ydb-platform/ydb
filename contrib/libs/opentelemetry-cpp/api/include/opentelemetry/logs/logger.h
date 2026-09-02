// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stdint.h>
#include <type_traits>
#include <utility>

#include "opentelemetry/context/context.h"
#include "opentelemetry/logs/event_id.h"
#include "opentelemetry/logs/logger_type_traits.h"
#include "opentelemetry/logs/severity.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/unique_ptr.h"
#include "opentelemetry/version.h"

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
#  include "opentelemetry/context/runtime_context.h"
#  include "opentelemetry/nostd/variant.h"
#  include "opentelemetry/trace/span_context.h"
#  include "opentelemetry/trace/trace_flags.h"
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

OPENTELEMETRY_BEGIN_NAMESPACE
namespace common
{
class KeyValueIterable;
}  // namespace common

namespace logs
{

class EventId;
class LogRecord;

/**
 * Handles log record creation.
 **/
class Logger
{
public:
  Logger()                              = default;
  Logger(const Logger &)                = default;
  Logger(Logger &&) noexcept            = default;
  Logger &operator=(const Logger &)     = default;
  Logger &operator=(Logger &&) noexcept = default;
  virtual ~Logger()                     = default;

  /* Returns the name of the logger */
  virtual const nostd::string_view GetName() noexcept = 0;

  /**
   * Create a Log Record object
   *
   * @return nostd::unique_ptr<LogRecord>
   */
  virtual nostd::unique_ptr<LogRecord> CreateLogRecord() noexcept = 0;

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  /**
   * Create a Log Record object using either a Context or a SpanContext.
   *
   * @param context_or_span Variant carrying either a full Context or just a
   *                        SpanContext.
   * @return nostd::unique_ptr<LogRecord>
   */
  virtual nostd::unique_ptr<LogRecord> CreateLogRecord(
      const nostd::variant<trace::SpanContext, opentelemetry::context::Context> &
      /*context_or_span*/) noexcept
  {
    return CreateLogRecord();
  }
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

  /**
   * Emit a Log Record object
   *
   * @param log_record Log record
   */
  virtual void EmitLogRecord(nostd::unique_ptr<LogRecord> &&log_record) noexcept = 0;

  /**
   * Emit a Log Record object with arguments.
   *
   * @note This overload does NOT apply the @c Enabled filter chain. Callers who
   *       constructed @p log_record themselves are responsible for calling
   *       @c Enabled(severity, ...) before invoking this overload if they want
   *       the LoggerConfig filtering rules (minimum severity, trace-based,
   *       processor.Enabled) to be honored. In ABI v2 builds, the no-record
   *       overload @c EmitLogRecord(args...) below does call the filter chain
   *       automatically when @c Severity is present in @p args; in ABI v1
   *       builds neither overload applies the filter chain.
   *
   * @param log_record Log record
   * @param args Arguments which can be used to set data of log record by type.
   *  Severity                                -> severity, severity_text
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   */
  template <class... ArgumentType>
  void EmitLogRecord(nostd::unique_ptr<LogRecord> &&log_record, ArgumentType &&...args)
  {
    if (!log_record)
    {
      return;
    }

    //
    // Keep the parameter pack unpacking order from left to right because left
    // ones are usually more important like severity and event_id than the
    // attributes. The left to right unpack order could pass the more important
    // data to processors to avoid caching and memory allocating.
    //
#if __cplusplus <= 201402L
    // C++14 does not support fold expressions for parameter pack expansion.
    int dummy[] = {(detail::LogRecordSetterTrait<typename std::decay<ArgumentType>::type>::Set(
                        log_record.get(), std::forward<ArgumentType>(args)),
                    0)...};
    IgnoreTraitResult(dummy);
#else
    IgnoreTraitResult((detail::LogRecordSetterTrait<typename std::decay<ArgumentType>::type>::Set(
                           log_record.get(), std::forward<ArgumentType>(args)),
                       ...));
#endif

    EmitLogRecord(std::move(log_record));
  }

  /**
   * Emit a Log Record object with arguments
   *
   * @param args Arguments which can be used to set data of log record by type.
   *  Severity                                -> severity, severity_text
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   *  Context (v2 only)                       -> filter + trace stamp (recommended: pass last)
   *
   *  When a @c Context or trace parts (@c SpanContext, or @c TraceId +
   *  @c SpanId [+ @c TraceFlags]) are in args, the filter chain uses
   *  @c Enabled(context_or_span, severity, ...) and the record is created
   *  via @c CreateLogRecord(context_or_span), so the filter evaluates
   *  against the trace this record is for instead of the implicit runtime
   *  context.
   */
  template <class... ArgumentType>
  void EmitLogRecord(ArgumentType &&...args)
  {
#if OPENTELEMETRY_ABI_VERSION_NO >= 2
    const Severity arg_severity = detail::FindSeverityInArgs(args...);
    if (arg_severity != Severity::kInvalid && !Enabled(arg_severity))
    {
      return;
    }

    nostd::variant<trace::SpanContext, opentelemetry::context::Context> context_or_span =
        trace::SpanContext::GetInvalid();

    if (const opentelemetry::context::Context *ctx = detail::FindContextInArgs(args...))
    {
      context_or_span = *ctx;
    }
    else if (const trace::SpanContext *sc = detail::FindSpanContextInArgs(args...))
    {
      context_or_span = *sc;
    }
    else
    {
      const trace::TraceId *trace_id_ptr = detail::FindTraceIdInArgs(args...);
      const trace::SpanId *span_id_ptr   = detail::FindSpanIdInArgs(args...);
      if (trace_id_ptr != nullptr && span_id_ptr != nullptr)
      {
        const trace::TraceFlags *trace_flags_ptr = detail::FindTraceFlagsInArgs(args...);
        context_or_span                          = trace::SpanContext(
            *trace_id_ptr, *span_id_ptr,
            trace_flags_ptr != nullptr ? *trace_flags_ptr : trace::TraceFlags{}, false);
      }
      else
      {
        context_or_span = opentelemetry::context::RuntimeContext::GetCurrent();
      }
    }

    if (arg_severity != Severity::kInvalid)
    {
      const EventId *event_id_ptr = detail::FindEventIdInArgs(args...);
      if (ExtendedEnabledRequired())
      {
        const bool extended_enabled =
            event_id_ptr ? EnabledImplementation(context_or_span, arg_severity, *event_id_ptr)
                         : EnabledImplementation(context_or_span, arg_severity);
        if (!extended_enabled)
        {
          return;
        }
      }
    }

    nostd::unique_ptr<LogRecord> log_record = CreateLogRecord(context_or_span);
#else
    nostd::unique_ptr<LogRecord> log_record = CreateLogRecord();
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

    EmitLogRecord(std::move(log_record), std::forward<ArgumentType>(args)...);
  }

  /**
   * Writes a log with a severity of trace.
   * @param args Arguments which can be used to set data of log record by type.
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   */
  template <class... ArgumentType>
  void Trace(ArgumentType &&...args) noexcept
  {
    static_assert(
        !detail::LogRecordHasType<Severity, typename std::decay<ArgumentType>::type...>::value,
        "Severity is already set.");
    this->EmitLogRecord(Severity::kTrace, std::forward<ArgumentType>(args)...);
  }

  /**
   * Writes a log with a severity of debug.
   * @param args Arguments which can be used to set data of log record by type.
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   */
  template <class... ArgumentType>
  void Debug(ArgumentType &&...args) noexcept
  {
    static_assert(
        !detail::LogRecordHasType<Severity, typename std::decay<ArgumentType>::type...>::value,
        "Severity is already set.");
    this->EmitLogRecord(Severity::kDebug, std::forward<ArgumentType>(args)...);
  }

  /**
   * Writes a log with a severity of info.
   * @param args Arguments which can be used to set data of log record by type.
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   */
  template <class... ArgumentType>
  void Info(ArgumentType &&...args) noexcept
  {
    static_assert(
        !detail::LogRecordHasType<Severity, typename std::decay<ArgumentType>::type...>::value,
        "Severity is already set.");
    this->EmitLogRecord(Severity::kInfo, std::forward<ArgumentType>(args)...);
  }

  /**
   * Writes a log with a severity of warn.
   * @param args Arguments which can be used to set data of log record by type.
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   */
  template <class... ArgumentType>
  void Warn(ArgumentType &&...args) noexcept
  {
    static_assert(
        !detail::LogRecordHasType<Severity, typename std::decay<ArgumentType>::type...>::value,
        "Severity is already set.");
    this->EmitLogRecord(Severity::kWarn, std::forward<ArgumentType>(args)...);
  }

  /**
   * Writes a log with a severity of error.
   * @param args Arguments which can be used to set data of log record by type.
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   */
  template <class... ArgumentType>
  void Error(ArgumentType &&...args) noexcept
  {
    static_assert(
        !detail::LogRecordHasType<Severity, typename std::decay<ArgumentType>::type...>::value,
        "Severity is already set.");
    this->EmitLogRecord(Severity::kError, std::forward<ArgumentType>(args)...);
  }

  /**
   * Writes a log with a severity of fatal.
   * @param args Arguments which can be used to set data of log record by type.
   *  string_view                             -> body
   *  AttributeValue                          -> body
   *  SpanContext                             -> span_id,trace_id and trace_flags
   *  SpanId                                  -> span_id
   *  TraceId                                 -> trace_id
   *  TraceFlags                              -> trace_flags
   *  SystemTimestamp                         -> timestamp
   *  system_clock::time_point                -> timestamp
   *  KeyValueIterable                        -> attributes
   *  Key value iterable container            -> attributes
   *  span<pair<string_view, AttributeValue>> -> attributes(return type of MakeAttributes)
   */
  template <class... ArgumentType>
  void Fatal(ArgumentType &&...args) noexcept
  {
    static_assert(
        !detail::LogRecordHasType<Severity, typename std::decay<ArgumentType>::type...>::value,
        "Severity is already set.");
    this->EmitLogRecord(Severity::kFatal, std::forward<ArgumentType>(args)...);
  }

  //
  // OpenTelemetry C++ user-facing Logs API
  //

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  inline bool Enabled(
      const nostd::variant<trace::SpanContext, opentelemetry::context::Context> &context_or_span,
      Severity severity = Severity::kInvalid) const noexcept
  {
    if OPENTELEMETRY_LIKELY_CONDITION (!Enabled(severity))
    {
      return false;
    }
    return EnabledImplementation(context_or_span, severity);
  }

  inline bool Enabled(
      const nostd::variant<trace::SpanContext, opentelemetry::context::Context> &context_or_span,
      Severity severity,
      const EventId &event_id) const noexcept
  {
    if OPENTELEMETRY_LIKELY_CONDITION (!Enabled(severity))
    {
      return false;
    }
    return EnabledImplementation(context_or_span, severity, event_id);
  }
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

  inline bool Enabled(Severity severity, const EventId &event_id) const noexcept
  {
    if OPENTELEMETRY_LIKELY_CONDITION (!Enabled(severity))
    {
      return false;
    }
    return EnabledImplementation(severity, event_id);
  }

  inline bool Enabled(Severity severity, int64_t event_id) const noexcept
  {
    if OPENTELEMETRY_LIKELY_CONDITION (!Enabled(severity))
    {
      return false;
    }
    return EnabledImplementation(severity, event_id);
  }

  inline bool Enabled(Severity severity) const noexcept
  {
    return static_cast<uint8_t>(severity) >= OPENTELEMETRY_ATOMIC_READ_8(&minimum_severity_);
  }

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  inline bool ExtendedEnabledRequired() const noexcept
  {
    return OPENTELEMETRY_ATOMIC_READ_8(&extended_enabled_required_) != 0;
  }
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

  /**
   * Log an event
   *
   * @param severity severity of the log
   * @param event_id event identifier of the log
   * @param format an utf-8 string following https://messagetemplates.org/
   * @param attributes key value pairs of the log
   */
  virtual void Log(Severity severity,
                   const EventId &event_id,
                   nostd::string_view format,
                   const common::KeyValueIterable &attributes) noexcept
  {
    this->EmitLogRecord(severity, event_id, format, attributes);
  }

  virtual void Log(Severity severity,
                   int64_t event_id,
                   nostd::string_view format,
                   const common::KeyValueIterable &attributes) noexcept
  {
    this->EmitLogRecord(severity, EventId{event_id}, format, attributes);
  }

  virtual void Log(Severity severity,
                   nostd::string_view format,
                   const common::KeyValueIterable &attributes) noexcept
  {
    this->EmitLogRecord(severity, format, attributes);
  }

  virtual void Log(Severity severity, nostd::string_view message) noexcept
  {
    this->EmitLogRecord(severity, message);
  }

  // Convenient wrappers based on virtual methods Log().
  // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md#field-severitynumber

  inline void Trace(const EventId &event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kTrace, event_id, format, attributes);
  }

  inline void Trace(int64_t event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kTrace, EventId{event_id}, format, attributes);
  }

  inline void Trace(nostd::string_view format, const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kTrace, format, attributes);
  }

  inline void Trace(nostd::string_view message) noexcept { this->Log(Severity::kTrace, message); }

  inline void Debug(const EventId &event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kDebug, event_id, format, attributes);
  }

  inline void Debug(int64_t event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kDebug, EventId{event_id}, format, attributes);
  }

  inline void Debug(nostd::string_view format, const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kDebug, format, attributes);
  }

  inline void Debug(nostd::string_view message) noexcept { this->Log(Severity::kDebug, message); }

  inline void Info(const EventId &event_id,
                   nostd::string_view format,
                   const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kInfo, event_id, format, attributes);
  }

  inline void Info(int64_t event_id,
                   nostd::string_view format,
                   const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kInfo, EventId{event_id}, format, attributes);
  }

  inline void Info(nostd::string_view format, const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kInfo, format, attributes);
  }

  inline void Info(nostd::string_view message) noexcept { this->Log(Severity::kInfo, message); }

  inline void Warn(const EventId &event_id,
                   nostd::string_view format,
                   const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kWarn, event_id, format, attributes);
  }

  inline void Warn(int64_t event_id,
                   nostd::string_view format,
                   const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kWarn, EventId{event_id}, format, attributes);
  }

  inline void Warn(nostd::string_view format, const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kWarn, format, attributes);
  }

  inline void Warn(nostd::string_view message) noexcept { this->Log(Severity::kWarn, message); }

  inline void Error(const EventId &event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kError, event_id, format, attributes);
  }

  inline void Error(int64_t event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kError, EventId{event_id}, format, attributes);
  }

  inline void Error(nostd::string_view format, const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kError, format, attributes);
  }

  inline void Error(nostd::string_view message) noexcept { this->Log(Severity::kError, message); }

  inline void Fatal(const EventId &event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kFatal, event_id, format, attributes);
  }

  inline void Fatal(int64_t event_id,
                    nostd::string_view format,
                    const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kFatal, EventId{event_id}, format, attributes);
  }

  inline void Fatal(nostd::string_view format, const common::KeyValueIterable &attributes) noexcept
  {
    this->Log(Severity::kFatal, format, attributes);
  }

  inline void Fatal(nostd::string_view message) noexcept { this->Log(Severity::kFatal, message); }

  //
  // End of OpenTelemetry C++ user-facing Log API.
  //

protected:
  virtual bool EnabledImplementation(Severity /*severity*/,
                                     const EventId & /*event_id*/) const noexcept
  {
    return false;
  }

  virtual bool EnabledImplementation(Severity /*severity*/, int64_t /*event_id*/) const noexcept
  {
    return false;
  }

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  virtual bool EnabledImplementation(
      const nostd::variant<trace::SpanContext, opentelemetry::context::Context> &
      /*context_or_span*/,
      Severity /*severity*/) const noexcept
  {
    return false;
  }

  virtual bool EnabledImplementation(
      const nostd::variant<trace::SpanContext, opentelemetry::context::Context> &
      /*context_or_span*/,
      Severity /*severity*/,
      const EventId & /*event_id*/) const noexcept
  {
    return false;
  }
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

  void SetMinimumSeverity(uint8_t severity_or_max) noexcept
  {
    OPENTELEMETRY_ATOMIC_WRITE_8(&minimum_severity_, severity_or_max);
  }

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  void SetExtendedEnabledRequired(bool required) noexcept
  {
    OPENTELEMETRY_ATOMIC_WRITE_8(&extended_enabled_required_,
                                 static_cast<uint8_t>(required ? 1 : 0));
  }
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

private:
  template <class... ValueType>
  void IgnoreTraitResult(ValueType &&...)
  {}

  //
  // minimum_severity_ can be updated concurrently by multiple threads/cores, so race condition on
  // read/write should be handled. And std::atomic can not be used here because it is not ABI
  // compatible for OpenTelemetry C++ API.
  //
  mutable uint8_t minimum_severity_{kMaxSeverity};

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  //
  // Controls whether the EmitLogRecord(args...) template calls the
  // EnabledImplementation virtual in addition to the cheap atomic
  // minimum_severity_ check. When 0, the template trusts the atomic check
  // as the complete enabled decision and skips the virtual dispatch. When
  // 1, the template also consults EnabledImplementation, which lets a
  // Logger subclass apply richer filtering (trace-based, processor-level
  // Enabled, custom predicates).
  //
  mutable uint8_t extended_enabled_required_{0};
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2
};
}  // namespace logs
OPENTELEMETRY_END_NAMESPACE
