#pragma once

#include <ydb/library/protobuf_printer/security_printer.h>

#include <library/cpp/logger/priority.h>

#include <util/generic/ptr.h>
#include <util/system/env.h>


namespace NYdbGrpc {

static bool LogBodyEnabled = "BODY" == GetEnv("YDB_GRPC_SERVER_LOGGING");

class TLogger: public TThrRefBase {
protected:
    TLogger() = default;

public:
    [[nodiscard]]
    bool IsEnabled(ELogPriority priority) const noexcept {
        return DoIsEnabled(priority);
    }

    void Y_PRINTF_FORMAT(3, 4) Write(ELogPriority priority, const char* format, ...) noexcept {
        va_list args;
        va_start(args, format);
        DoWrite(priority, format, args);
        va_end(args);
    }

protected:
    virtual bool DoIsEnabled(ELogPriority priority) const noexcept = 0;
    virtual void DoWrite(ELogPriority p, const char* format, va_list args) noexcept  = 0;
};

using TLoggerPtr = TIntrusivePtr<TLogger>;

#define GRPC_LOG_DEBUG(logger, format, ...) \
    if (logger && logger->IsEnabled(ELogPriority::TLOG_DEBUG)) { \
        logger->Write(ELogPriority::TLOG_DEBUG, format, __VA_ARGS__); \
    } else { }

#define GRPC_LOG_INFO(logger, format, ...) \
    if (logger && logger->IsEnabled(ELogPriority::TLOG_INFO)) { \
        logger->Write(ELogPriority::TLOG_INFO, format, __VA_ARGS__); \
    } else { }

template <typename TMsg>
inline TString FormatMessage(const TMsg& message, bool ok = true) {
    if (ok) {
        if (LogBodyEnabled) {
            return NKikimr::SecureDebugString<TMsg>(message);
        } else {
            return "<hidden>";
        }
    } else {
        return "<not ok>";
    }
}

} // namespace NYdbGrpc
