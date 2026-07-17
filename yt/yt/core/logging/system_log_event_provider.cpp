#include "system_log_event_provider.h"
#include "config.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/logging/tagged_payload.h>
#include <library/cpp/yt/logging/structured_payload.h>

namespace NYT::NLogging {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, SystemLoggingCategoryName);

////////////////////////////////////////////////////////////////////////////////

class TDisabledSystemLogEventProvider
    : public ISystemLogEventProvider
{
public:
    std::optional<TLogEvent> GetStartLogEvent() const override
    {
        return {};
    }

    std::optional<TLogEvent> GetSkippedLogEvent(i64 /*count*/, TStringBuf /*skippedBy*/) const override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPlainTextSystemLogEventProvider
    : public ISystemLogEventProvider
{
public:
    std::optional<TLogEvent> GetStartLogEvent() const override
    {
        if (!GetDefaultLogManager()) {
            return {};
        }

        return TLogEvent{
            .Category = Logger().GetCategory(),
            .Level = ELogLevel::Info,
            .Payload = MakeTaggedPayloadFromMessage(Format("Logging started (Version: %v, BuildHost: %v, BuildTime: %v)",
                GetVersion(),
                GetBuildHost(),
                GetBuildTime())),
            .Instant = GetCpuInstant(),
        };
    }

    std::optional<TLogEvent> GetSkippedLogEvent(i64 count, TStringBuf skippedBy) const override
    {
        if (count == 0 || !GetDefaultLogManager()) {
            return {};
        }

        return TLogEvent{
            .Category = Logger().GetCategory(),
            .Level = ELogLevel::Info,
            .Payload = MakeTaggedPayloadFromMessage(Format("Skipped log records in last second (Count: %v, SkippedBy: %v)",
                count,
                skippedBy)),
            .Instant = GetCpuInstant(),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStructuredSystemLogEventProvider
    : public ISystemLogEventProvider
{
public:
    std::optional<TLogEvent> GetStartLogEvent() const override
    {
        if (!GetDefaultLogManager()) {
            return {};
        }

        return TLogEvent{
            .Category = Logger().GetCategory(),
            .Level = ELogLevel::Info,
            .Payload = MakeStructuredPayloadFromYson(BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
                .Item("message").Value("Logging started")
                .Item("version").Value(GetVersion())
                .Item("build_host").Value(GetBuildHost())
                .Item("build_time").Value(GetBuildTime())
                .Finish()),
            .Instant = GetCpuInstant(),
        };
    }

    std::optional<TLogEvent> GetSkippedLogEvent(i64 count, TStringBuf skippedBy) const override
    {
        if (count == 0 || !GetDefaultLogManager()) {
            return {};
        }

        return TLogEvent{
            .Category = Logger().GetCategory(),
            .Level = ELogLevel::Info,
            .Payload = MakeStructuredPayloadFromYson(BuildYsonStringFluently<NYson::EYsonType::MapFragment>()
                .Item("message").Value("Events skipped")
                .Item("skipped_by").Value(skippedBy)
                .Item("events_skipped").Value(count)
                .Finish()),
            .Instant = GetCpuInstant(),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISystemLogEventProvider> CreateDefaultSystemLogEventProvider(
    bool systemMessagesEnabled,
    ELogFamily systemMessageFamily)
{
    if (!systemMessagesEnabled) {
        return std::make_unique<TDisabledSystemLogEventProvider>();
    }

    switch (systemMessageFamily) {
        case ELogFamily::PlainText:
            return std::make_unique<TPlainTextSystemLogEventProvider>();
        case ELogFamily::Structured:
            return std::make_unique<TStructuredSystemLogEventProvider>();
    }
}

std::unique_ptr<ISystemLogEventProvider> CreateDefaultSystemLogEventProvider(const TLogWriterConfigPtr& writerConfig)
{
    return CreateDefaultSystemLogEventProvider(
        writerConfig->AreSystemMessagesEnabled(),
        writerConfig->GetSystemMessageFamily());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
