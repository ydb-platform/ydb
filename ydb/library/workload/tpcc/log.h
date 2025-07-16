#pragma once

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

namespace NYdb::NTPCC {

using TLogProcessor = std::function<void(ELogPriority, const std::string&)>;

inline const char* PriorityToString(ELogPriority priority) {
    switch (priority) {
        case TLOG_EMERG:     return "EMERG";
        case TLOG_ALERT:     return "ALERT";
        case TLOG_CRIT:      return "CRIT";
        case TLOG_ERR:       return "ERROR";
        case TLOG_WARNING:   return "WARN";
        case TLOG_NOTICE:    return "NOTICE";
        case TLOG_INFO:      return "INFO";
        case TLOG_DEBUG:     return "DEBUG";
        case TLOG_RESOURCES: return "TRACE";
        default:             return "UNKNOWN";
    }
}

inline TStringBuf GetLogColor(ELogPriority priority) {
    switch (priority) {
        case TLOG_EMERG:
            [[fallthrough]];
        case TLOG_ALERT:
            [[fallthrough]];
        case TLOG_CRIT:
            [[fallthrough]];
        case TLOG_ERR:
            return NColorizer::StdErr().RedColor();
        case TLOG_WARNING:
            return NColorizer::StdErr().YellowColor();
        case TLOG_NOTICE:
            [[fallthrough]];
        case TLOG_INFO:
            [[fallthrough]];
        case TLOG_DEBUG:
            [[fallthrough]];
        case TLOG_RESOURCES:
            [[fallthrough]];
        default:
            return NColorizer::StdErr().Default();
    }
}

inline size_t GetLenOfFormatDate8601Part() {
    return 20;
}

inline size_t GetOffsetToLogMessage(ELogPriority priority) {
    return GetLenOfFormatDate8601Part() + GetLogColor(priority).size() + strlen(PriorityToString(priority))
        + NColorizer::StdErr().Default().size() + 1; // 1 is extra space after dt we add below
}

} // namespace NYdb::NTPCC

#define LOG_IMPL(log, level, message) \
    if (log->FiltrationLevel() >= level) { \
        char buf[DATE_8601_LEN];           \
        log->Write(level, TStringBuilder() \
            << TStringBuf(buf, FormatDate8601(buf, sizeof(buf), TInstant::Now().Seconds())) \
            << " " << GetLogColor(level) << PriorityToString(level) << NColorizer::StdErr().Default() \
            << ": " << message << Endl); \
    } \
    Y_SEMICOLON_GUARD

#define LOG_T(message) LOG_IMPL(Log, ELogPriority::TLOG_RESOURCES, message)
#define LOG_D(message) LOG_IMPL(Log, ELogPriority::TLOG_DEBUG, message)
#define LOG_I(message) LOG_IMPL(Log, ELogPriority::TLOG_INFO, message)
#define LOG_W(message) LOG_IMPL(Log, ELogPriority::TLOG_WARNING, message)
#define LOG_E(message) LOG_IMPL(Log, ELogPriority::TLOG_ERR, message)
