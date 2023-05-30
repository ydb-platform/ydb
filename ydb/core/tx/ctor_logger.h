#pragma once

#include <library/cpp/actors/core/log.h>
#include <ydb/core/protos/services.pb.h>

// You have to define TLogThis in local namespace to use these macros
#define LOG_S_EMERG(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_EMERG, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_ALERT(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_ALERT, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_CRIT(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_CRIT, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_ERROR(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_ERROR, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_WARN(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_WARN, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_NOTICE(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_NOTICE, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_INFO(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_INFO, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_DEBUG(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_DEBUG, [&](TStringBuilder& ss){ ss << stream; }); }
#define LOG_S_TRACE(stream) { TLogThis(*TlsActivationContext, NActors::NLog::PRI_TRACE, [&](TStringBuilder& ss){ ss << stream; }); }

namespace NKikimr {

template <NKikimrServices::EServiceKikimr Component>
class TCtorLogger
{
public:
    template <typename TFunc>
    TCtorLogger(const NActors::TActivationContext& ctx, NActors::NLog::EPriority priority, TFunc logFunc)
    {
        if (IS_LOG_PRIORITY_ENABLED(priority, Component)) {
            TStringBuilder strStream;
            logFunc(strStream);
            ::NActors::MemLogAdapter(ctx, priority, Component, "%s", strStream.data());
        }
    }
};

}
