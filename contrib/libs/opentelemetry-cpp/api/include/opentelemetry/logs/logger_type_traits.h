// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>
#include <memory>
#include <type_traits>

#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/common/timestamp.h"
#include "opentelemetry/logs/event_id.h"
#include "opentelemetry/logs/log_record.h"
#include "opentelemetry/logs/severity.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/type_traits.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_id.h"
#include "opentelemetry/trace/trace_flags.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/version.h"

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
#  include "opentelemetry/context/context.h"
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

OPENTELEMETRY_BEGIN_NAMESPACE
namespace logs
{
namespace detail
{
template <class ValueType>
struct LogRecordSetterTrait;

template <>
struct LogRecordSetterTrait<Severity>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    log_record->SetSeverity(std::forward<ArgumentType>(arg));

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<EventId>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, const ArgumentType &arg) noexcept
  {
    log_record->SetEventId(arg.id_, nostd::string_view{arg.name_.get()});

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<trace::SpanContext>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, const ArgumentType &arg) noexcept
  {
    log_record->SetSpanId(arg.span_id());
    log_record->SetTraceId(arg.trace_id());
    log_record->SetTraceFlags(arg.trace_flags());

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<trace::SpanId>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    log_record->SetSpanId(std::forward<ArgumentType>(arg));

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<trace::TraceId>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    log_record->SetTraceId(std::forward<ArgumentType>(arg));

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<trace::TraceFlags>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    log_record->SetTraceFlags(std::forward<ArgumentType>(arg));

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<common::SystemTimestamp>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    log_record->SetTimestamp(std::forward<ArgumentType>(arg));

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<std::chrono::system_clock::time_point>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    log_record->SetTimestamp(common::SystemTimestamp(std::forward<ArgumentType>(arg)));

    return log_record;
  }
};

template <>
struct LogRecordSetterTrait<common::KeyValueIterable>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, const ArgumentType &arg) noexcept
  {
    arg.ForEachKeyValue(
        [&log_record](nostd::string_view key, common::AttributeValue value) noexcept {
          log_record->SetAttribute(key, value);
          return true;
        });

    return log_record;
  }
};

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
// Context in the argument list is consumed by EmitLogRecord(args...) for
// context-aware filtering and trace stamping (see FindContextInArgs); it is
// not a record field, so the setter is a no-op.
template <>
struct LogRecordSetterTrait<opentelemetry::context::Context>
{
  template <class ArgumentType>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType && /*arg*/) noexcept
  {
    return log_record;
  }
};
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

template <class ValueType>
struct LogRecordSetterTrait
{
  static_assert(!std::is_same<nostd::unique_ptr<LogRecord>, ValueType>::value &&
                    !std::is_same<std::unique_ptr<LogRecord>, ValueType>::value,
                "unique_ptr<LogRecord> is not allowed, please use std::move()");

  template <class ArgumentType,
            nostd::enable_if_t<std::is_convertible<ArgumentType, nostd::string_view>::value ||
                                   std::is_convertible<ArgumentType, common::AttributeValue>::value,
                               void> * = nullptr>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    log_record->SetBody(std::forward<ArgumentType>(arg));

    return log_record;
  }

  template <class ArgumentType,
            nostd::enable_if_t<std::is_base_of<common::KeyValueIterable, ArgumentType>::value, bool>
                * = nullptr>
  inline static LogRecord *Set(LogRecord *log_record, ArgumentType &&arg) noexcept
  {
    return LogRecordSetterTrait<common::KeyValueIterable>::Set(log_record,
                                                               std::forward<ArgumentType>(arg));
  }

  template <class ArgumentType,
            nostd::enable_if_t<common::detail::is_key_value_iterable<ArgumentType>::value, int> * =
                nullptr>
  inline static LogRecord *Set(LogRecord *log_record, const ArgumentType &arg) noexcept
  {
    for (auto &argv : arg)
    {
      log_record->SetAttribute(argv.first, argv.second);
    }

    return log_record;
  }
};

template <class ValueType, class... ArgumentType>
struct LogRecordHasType;

template <class ValueType>
struct LogRecordHasType<ValueType> : public std::false_type
{};

template <class ValueType, class TargetType, class... ArgumentType>
struct LogRecordHasType<ValueType, TargetType, ArgumentType...>
    : public std::conditional<std::is_same<ValueType, TargetType>::value,
                              std::true_type,
                              LogRecordHasType<ValueType, ArgumentType...>>::type
{};

inline Severity FindSeverityInArgs() noexcept
{
  return Severity::kInvalid;
}

template <class... Rest>
inline Severity FindSeverityInArgs(Severity severity, Rest &&.../*rest*/) noexcept
{
  return severity;
}

template <class First,
          class... Rest,
          typename std::enable_if<!std::is_same<typename std::decay<First>::type, Severity>::value,
                                  int>::type = 0>
inline Severity FindSeverityInArgs(First && /*first*/, Rest &&...rest) noexcept
{
  return FindSeverityInArgs(std::forward<Rest>(rest)...);
}

inline const EventId *FindEventIdInArgs() noexcept
{
  return nullptr;
}

template <class... Rest>
inline const EventId *FindEventIdInArgs(const EventId &event_id
                                            OPENTELEMETRY_ATTRIBUTE_LIFETIME_BOUND,
                                        Rest &&.../*rest*/) noexcept
{
  return &event_id;
}

template <class First,
          class... Rest,
          typename std::enable_if<!std::is_same<typename std::decay<First>::type, EventId>::value,
                                  int>::type = 0>
inline const EventId *FindEventIdInArgs(First && /*first*/, Rest &&...rest) noexcept
{
  return FindEventIdInArgs(std::forward<Rest>(rest)...);
}

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
inline const opentelemetry::context::Context *FindContextInArgs() noexcept
{
  return nullptr;
}

template <class... Rest>
inline const opentelemetry::context::Context *FindContextInArgs(
    const opentelemetry::context::Context &context OPENTELEMETRY_ATTRIBUTE_LIFETIME_BOUND,
    Rest &&.../*rest*/) noexcept
{
  return &context;
}

template <class First,
          class... Rest,
          typename std::enable_if<!std::is_same<typename std::decay<First>::type,
                                                opentelemetry::context::Context>::value,
                                  int>::type = 0>
inline const opentelemetry::context::Context *FindContextInArgs(First && /*first*/,
                                                                Rest &&...rest) noexcept
{
  return FindContextInArgs(std::forward<Rest>(rest)...);
}

inline const trace::SpanContext *FindSpanContextInArgs() noexcept
{
  return nullptr;
}

template <class... Rest>
inline const trace::SpanContext *FindSpanContextInArgs(const trace::SpanContext &span_context
                                                           OPENTELEMETRY_ATTRIBUTE_LIFETIME_BOUND,
                                                       Rest &&.../*rest*/) noexcept
{
  return &span_context;
}

template <class First,
          class... Rest,
          typename std::enable_if<
              !std::is_same<typename std::decay<First>::type, trace::SpanContext>::value,
              int>::type = 0>
inline const trace::SpanContext *FindSpanContextInArgs(First && /*first*/, Rest &&...rest) noexcept
{
  return FindSpanContextInArgs(std::forward<Rest>(rest)...);
}

inline const trace::TraceId *FindTraceIdInArgs() noexcept
{
  return nullptr;
}

template <class... Rest>
inline const trace::TraceId *FindTraceIdInArgs(const trace::TraceId &trace_id
                                                   OPENTELEMETRY_ATTRIBUTE_LIFETIME_BOUND,
                                               Rest &&.../*rest*/) noexcept
{
  return &trace_id;
}

template <
    class First,
    class... Rest,
    typename std::enable_if<!std::is_same<typename std::decay<First>::type, trace::TraceId>::value,
                            int>::type = 0>
inline const trace::TraceId *FindTraceIdInArgs(First && /*first*/, Rest &&...rest) noexcept
{
  return FindTraceIdInArgs(std::forward<Rest>(rest)...);
}

inline const trace::SpanId *FindSpanIdInArgs() noexcept
{
  return nullptr;
}

template <class... Rest>
inline const trace::SpanId *FindSpanIdInArgs(const trace::SpanId &span_id
                                                 OPENTELEMETRY_ATTRIBUTE_LIFETIME_BOUND,
                                             Rest &&.../*rest*/) noexcept
{
  return &span_id;
}

template <
    class First,
    class... Rest,
    typename std::enable_if<!std::is_same<typename std::decay<First>::type, trace::SpanId>::value,
                            int>::type = 0>
inline const trace::SpanId *FindSpanIdInArgs(First && /*first*/, Rest &&...rest) noexcept
{
  return FindSpanIdInArgs(std::forward<Rest>(rest)...);
}

inline const trace::TraceFlags *FindTraceFlagsInArgs() noexcept
{
  return nullptr;
}

template <class... Rest>
inline const trace::TraceFlags *FindTraceFlagsInArgs(const trace::TraceFlags &trace_flags
                                                         OPENTELEMETRY_ATTRIBUTE_LIFETIME_BOUND,
                                                     Rest &&.../*rest*/) noexcept
{
  return &trace_flags;
}

template <class First,
          class... Rest,
          typename std::enable_if<
              !std::is_same<typename std::decay<First>::type, trace::TraceFlags>::value,
              int>::type = 0>
inline const trace::TraceFlags *FindTraceFlagsInArgs(First && /*first*/, Rest &&...rest) noexcept
{
  return FindTraceFlagsInArgs(std::forward<Rest>(rest)...);
}
#endif  // OPENTELEMETRY_ABI_VERSION_NO >= 2

}  // namespace detail

}  // namespace logs
OPENTELEMETRY_END_NAMESPACE
