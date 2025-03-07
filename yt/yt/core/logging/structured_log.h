#pragma once

// This file implements a user-friendly API for structured logging. The API
// enables logging messages with structured key/value pairs with minimal
// syntactical overhead at the log site.
//
// Example:
//
//   YT_SLOG_EVENT(Logger(), ELogLevel::Info,  "an event occured!", ("event_type", event.type()));
//   YT_SLOG_EVENT(Logger(), ELogLevel::Error, "processing failed!", ("task", task.id()), ("error", error.msg()));

#include "log.h"

#include <util/generic/va_args.h>

#include <string_view>
#include <tuple>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

template <typename... Values>
void LogStructuredEvent(
    const TLogger& logger,
    ELogLevel level,
    std::string_view message,
    std::tuple<const char*, Values>&&... tags);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define YT_SLOG_TUPLE(x) std::make_tuple x,
#define YT_SLOG_TUPLE_LAST(x) std::make_tuple x

// YT_SLOG_EVENT enforces that primary structured log messages are known at
// compile-time, because structured errors (and logs in general) are usually
// aggregated by the primary log message. Put all dynamic data in structured
// tags instead of constructing the message.
//
// If an error about constant contexts points you here, you probably want to write
//
//     YT_SLOG_EVENT(Logger(), ELogLevel::Error, "Invalid request!", ("reqid", request.GetID()));
//
// instead of something like
//
//     YT_SLOG_EVENT(Logger(), ELogLevel::Error, "Invalid request: " + request.GetID());
#define YT_SLOG_EVENT(logger, level, message, ...)                                                   \
    do {                                                                                             \
        const auto& logger__ = (logger)();                                                           \
        auto level__ = (level);                                                                      \
        if (!logger__.IsLevelEnabled(level__)) {                                                     \
            break;                                                                                   \
        }                                                                                            \
        constexpr std::string_view message__(message);                                               \
        ::NYT::NLogging::LogStructuredEvent(                                                         \
            logger__,                                                                                \
            level__,                                                                                 \
            message__                                                                                \
                __VA_OPT__(, Y_MAP_ARGS_WITH_LAST(YT_SLOG_TUPLE, YT_SLOG_TUPLE_LAST, __VA_ARGS__))); \
    } while (false)

#define STRUCTURED_LOG_INL_H_
#include "structured_log-inl.h"
#undef STRUCTURED_LOG_INL_H_
