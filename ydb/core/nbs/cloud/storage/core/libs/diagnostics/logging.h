#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/startable.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/logger/priority.h>

#include <util/datetime/cputimer.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/printf.h>
#include <util/system/src_location.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TLogSettings
{
    ELogPriority FiltrationLevel = TLOG_INFO;
    bool UseLocalTimestamps = false;
    bool SuppressNewLine = false;
    TString BackendFileName = "";
};

////////////////////////////////////////////////////////////////////////////////

struct ILoggingService
    : public virtual IStartable
{
    virtual TLog CreateLog(const TString& component) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IAsyncLogger
    : public IStartable
{
    virtual void Enqueue(
        std::shared_ptr<TLogBackend> backend,
        ELogPriority logPriority,
        TString logRecord) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. Public method Kick() can be called from any thread.
struct TLogThrottler
{
    ui64 LastWrite = 0;
    const ui64 Period = 0;

    TLogThrottler(TDuration period)
        : Period(DurationToCyclesSafe(period))
    {}

    bool Kick()
    {
        auto now = GetCycleCount();
        auto lastWrite = AtomicGet(LastWrite);
        if (now - lastWrite >= Period) {
            return AtomicCas(&LastWrite, now, lastWrite);
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

TMaybe<ELogPriority> GetLogLevel(const TString& levelStr);

ILoggingServicePtr CreateLoggingService(
    const TString& logName,
    const TLogSettings& logSettings = {});

ILoggingServicePtr CreateLoggingService(
    std::shared_ptr<TLogBackend> backend,
    const TLogSettings& logSettings = {});

TLog CreateComponentLog(
    TString component,
    std::shared_ptr<TLogBackend> backend,
    IAsyncLoggerPtr asyncLogger,
    const TLogSettings& logSettings = {});

std::shared_ptr<TLogBackend> CreateMultiLogBackend(
    TVector<std::shared_ptr<TLogBackend>> backends);

IAsyncLoggerPtr CreateAsyncLogger();

ILoggingServicePtr CreateUnifiedAgentLoggingService(
    ILoggingServicePtr logging,
    const TString& endpoint,
    const TString& syslogService
);

////////////////////////////////////////////////////////////////////////////////
// TODO: move to util/system/defaults.h

// https://stackoverflow.com/questions/9183993/msvc-variadic-macro-expansion
#define Y_VA_SIZE_IMPL(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, TOTAL, ...) TOTAL
#define Y_VA_SIZE_EXPAND(args) Y_VA_SIZE_IMPL args
#define Y_VA_SIZE(...) Y_VA_SIZE_EXPAND((__VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1))

#define Y_VA_MACRO(MACRO, ...) Y_CAT(MACRO, Y_VA_SIZE(__VA_ARGS__)) (__VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_LOG_S(priority, stream)                                        \
    if (Log.IsOpen() && priority <= Log.FiltrationLevel()) {                   \
        Log << static_cast<ELogPriority>(priority)                             \
            << __LOCATION__ << ": " << stream;                                 \
    }                                                                          \
// STORAGE_LOG_S

#define STORAGE_LOG_F(priority, ...)                                           \
    if (Log.IsOpen() && priority <= Log.FiltrationLevel()) {                   \
        Printf(Log                                                             \
            << static_cast<ELogPriority>(priority)                             \
            << __LOCATION__ << ": ", __VA_ARGS__);                             \
    }                                                                          \
// STORAGE_LOG_F

#define STORAGE_LOG2  STORAGE_LOG_S
#define STORAGE_LOG3  STORAGE_LOG_F
#define STORAGE_LOG4  STORAGE_LOG_F
#define STORAGE_LOG5  STORAGE_LOG_F
#define STORAGE_LOG6  STORAGE_LOG_F
#define STORAGE_LOG7  STORAGE_LOG_F
#define STORAGE_LOG8  STORAGE_LOG_F
#define STORAGE_LOG9  STORAGE_LOG_F
#define STORAGE_LOG10 STORAGE_LOG_F

#define STORAGE_LOG(...) Y_VA_MACRO(STORAGE_LOG, __VA_ARGS__)

#define STORAGE_ERROR(...) STORAGE_LOG(TLOG_ERR,          __VA_ARGS__)
#define STORAGE_WARN(...)  STORAGE_LOG(TLOG_WARNING,      __VA_ARGS__)
#define STORAGE_INFO(...)  STORAGE_LOG(TLOG_INFO,         __VA_ARGS__)
#define STORAGE_DEBUG(...) STORAGE_LOG(TLOG_DEBUG,        __VA_ARGS__)
#define STORAGE_TRACE(...) STORAGE_LOG(TLOG_RESOURCES,    __VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_LOG_THROTTLED(throttler, ...)                                  \
    do {                                                                       \
        if ((throttler).Kick()) {                                              \
            STORAGE_LOG(__VA_ARGS__);                                          \
        }                                                                      \
    } while (0);
// STORAGE_LOG_THROTTLED

#define STORAGE_ERROR_T(throttler, ...)                                        \
    STORAGE_LOG_THROTTLED(throttler, TLOG_ERR, __VA_ARGS__)

#define STORAGE_WARN_T(throttler, ...)                                         \
    STORAGE_LOG_THROTTLED(throttler, TLOG_WARNING, __VA_ARGS__)

#define STORAGE_INFO_T(throttler, ...)                                         \
    STORAGE_LOG_THROTTLED(throttler, TLOG_INFO, __VA_ARGS__)

#define STORAGE_DEBUG_T(throttler, ...)                                        \
    STORAGE_LOG_THROTTLED(throttler, TLOG_DEBUG, __VA_ARGS__)

#define STORAGE_TRACE_T(throttler, ...)                                        \
    STORAGE_LOG_THROTTLED(throttler, TLOG_RESOURCES, __VA_ARGS__)

}   // namespace NCloud
