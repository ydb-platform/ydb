#pragma once

#include "util/generic/string.h"
#ifdef TRACE_LAZY
#error log macro redefinition
#endif

const TString TRACE_EVENT_MARKER = "TRACE_EVENT";

#define TRACE_LAZY(log, eventName, ...)                               \
    if (log.IsOpen() && log.FiltrationLevel() >= TLOG_RESOURCES) {    \
        TStringBuilder message;                                       \
        message << TRACE_EVENT_MARKER << ' ' << eventName << ' ';     \
        __VA_ARGS__;                                                  \
        log.Write(TLOG_RESOURCES, message);                           \
    }


#define TRACE_KV(key, value) (message << " " << (key) << "=" << (value))
#define TRACE_KV_IF(condition, key, value) (condition ? (message << " " << (key) << "=" << (value)) : message)
#define TRACE_IF(condition, ...) (condition ? __VA_ARGS__ : message)