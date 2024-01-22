#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <optional>

// You have to define TLogThis in local namespace to use these macros
#define __LOG_S_PRIORITY_IMPL__(stream, priority)                   \
    if (auto logThis = TLogThis(*TlsActivationContext, priority)) { \
        logThis.Str() << stream;                                    \
        logThis.WriteLog();                                         \
    }

#define LOG_S_EMERG(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_EMERG)
#define LOG_S_ALERT(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_ALERT)
#define LOG_S_CRIT(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_CRIT)
#define LOG_S_ERROR(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_ERROR)
#define LOG_S_WARN(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_WARN)
#define LOG_S_NOTICE(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_NOTICE)
#define LOG_S_INFO(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_INFO)
#define LOG_S_DEBUG(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_DEBUG)
#define LOG_S_TRACE(stream) __LOG_S_PRIORITY_IMPL__(stream, NActors::NLog::PRI_TRACE)

namespace NKikimr {

template <NKikimrServices::EServiceKikimr Component>
class TCtorLogger
{
    const NActors::TActivationContext& Ctx;
    NActors::NLog::EPriority Priority;
    std::optional<TStringBuilder> StrStream;

public:
    TCtorLogger(const NActors::TActivationContext& ctx, NActors::NLog::EPriority priority)
        : Ctx(ctx)
        , Priority(priority)
    {
    }

    operator bool() const {
        return IS_LOG_PRIORITY_ENABLED(Priority, Component);
    }

    TStringBuilder& Str() {
        StrStream.emplace();
        return *StrStream;
    }

    void WriteLog() {
        ::NActors::MemLogAdapter(Ctx, Priority, Component, std::move(*StrStream));
    }
};

}
