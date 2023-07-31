#pragma once

#include <util/generic/yexception.h>
#include <util/stream/output.h>

#include <cstdio>

namespace NReverseGeocoder {
    size_t const LOG_MESSAGE_LIMIT = 1024;

    enum ELogLevel {
        LOG_LEVEL_DISABLE = 0,
        LOG_LEVEL_ERROR,
        LOG_LEVEL_WARNING,
        LOG_LEVEL_INFO,
        LOG_LEVEL_DEBUG,
        LOG_LEVEL_COUNT
    };

    // Init logger. Setup OutputStream and logger level.
    void LogSetup(IOutputStream& out, ELogLevel level);

    // Write log message with colors, level and current time.
    // Example:
    //   (13:24:11.123456) Info: Good job!
    //   (13:24:11.323456) Warn: Ooops :(
    //   (13:24:22.456789) Error: Hello, world!
    void LogWrite(ELogLevel level, const char* message);

    // Log output file descriptor.
    IOutputStream& LogOutputStream();

    // Current log level.
    ELogLevel LogLevel();

    template <typename... TArgs>
    void LogWrite(ELogLevel level, const char* fmt, TArgs... args) {
        if (level <= LogLevel()) {
            char buffer[LOG_MESSAGE_LIMIT];
            // Ignore logger snprintf errors.
            snprintf(buffer, LOG_MESSAGE_LIMIT, fmt, std::forward<TArgs>(args)...);
            LogWrite(level, buffer);
        }
    }

    template <typename... TArgs>
    void LogError(TArgs... args) {
        LogWrite(LOG_LEVEL_ERROR, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    void LogWarning(TArgs... args) {
        LogWrite(LOG_LEVEL_WARNING, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    void LogInfo(TArgs... args) {
        LogWrite(LOG_LEVEL_INFO, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    void LogDebug(TArgs... args) {
        LogWrite(LOG_LEVEL_DEBUG, std::forward<TArgs>(args)...);
    }
}
