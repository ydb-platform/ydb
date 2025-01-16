#pragma once

#include <library/cpp/logger/log.h>

#define LOG_IMPL(log, level, message) \
    if (log->FiltrationLevel() >= level) { \
        log->Write(level, TStringBuilder() << message); \
    } \
    Y_SEMICOLON_GUARD

#define LOG_D(message) LOG_IMPL(Log, ELogPriority::TLOG_DEBUG, message)
#define LOG_I(message) LOG_IMPL(Log, ELogPriority::TLOG_INFO, message)
#define LOG_W(message) LOG_IMPL(Log, ELogPriority::TLOG_WARNING, message)
#define LOG_E(message) LOG_IMPL(Log, ELogPriority::TLOG_ERR, message)
