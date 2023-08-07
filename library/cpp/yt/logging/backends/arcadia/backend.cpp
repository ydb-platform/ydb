#include "backend.h"

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

ELogLevel ConvertToLogLevel(ELogPriority priority)
{
    switch (priority) {
        case ELogPriority::TLOG_DEBUG:
            return ELogLevel::Debug;
        case ELogPriority::TLOG_INFO:
            [[fallthrough]];
        case ELogPriority::TLOG_NOTICE:
            return ELogLevel::Info;
        case ELogPriority::TLOG_WARNING:
            return ELogLevel::Warning;
        case ELogPriority::TLOG_ERR:
            return ELogLevel::Error;
        case ELogPriority::TLOG_CRIT:
        case ELogPriority::TLOG_ALERT:
            return ELogLevel::Alert;
        case ELogPriority::TLOG_EMERG:
            return ELogLevel::Fatal;
        case ELogPriority::TLOG_RESOURCES:
            return ELogLevel::Maximum;
    }
    YT_ABORT();
}

class TLogBackendBridge
    : public TLogBackend
{
public:
    TLogBackendBridge(const TLogger& logger)
        : Logger_(logger)
    { }

    void WriteData(const TLogRecord& rec) override
    {
        const auto logLevel = ConvertToLogLevel(rec.Priority);
        if (!Logger_.IsLevelEnabled(logLevel)) {
            return;
        }

        // Remove trailing \n, because it will add it.
        TStringBuf message(rec.Data, rec.Len);
        message.ChopSuffix(TStringBuf("\n"));
        // Use low-level api, because it is more convinient here.
        auto loggingContext = GetLoggingContext();
        auto event = NDetail::CreateLogEvent(loggingContext, Logger_, logLevel);
        event.MessageRef = NDetail::BuildLogMessage(loggingContext, Logger_, message).MessageRef;
        event.Family = ELogFamily::PlainText;
        Logger_.Write(std::move(event));
    }

    void ReopenLog() override
    { }

    ELogPriority FiltrationLevel() const override
    {
        return LOG_MAX_PRIORITY;
    }

private:
    const TLogger Logger_;
};

} // namespace

THolder<TLogBackend> CreateArcadiaLogBackend(const TLogger& logger)
{
    return MakeHolder<TLogBackendBridge>(logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
