#pragma once

#include "defs.h"

#define QLOG_LOG_S(marker, priority, stream) LOG_LOG(ctx, priority, NKikimrServices::BS_QUEUE, "%s @%s: %s Marker# %s", \
    LogPrefix.data(), __func__, static_cast<TString>(TStringBuilder() << stream).data(), marker)

#define QLOG_EMERG_S(marker, arg)  QLOG_LOG_S(marker, NActors::NLog::PRI_EMERG , arg)
#define QLOG_ALERT_S(marker, arg)  QLOG_LOG_S(marker, NActors::NLog::PRI_ALERT , arg)
#define QLOG_CRIT_S(marker, arg)   QLOG_LOG_S(marker, NActors::NLog::PRI_CRIT  , arg)
#define QLOG_ERROR_S(marker, arg)  QLOG_LOG_S(marker, NActors::NLog::PRI_ERROR , arg)
#define QLOG_WARN_S(marker, arg)   QLOG_LOG_S(marker, NActors::NLog::PRI_WARN  , arg)
#define QLOG_NOTICE_S(marker, arg) QLOG_LOG_S(marker, NActors::NLog::PRI_NOTICE, arg)
#define QLOG_INFO_S(marker, arg)   QLOG_LOG_S(marker, NActors::NLog::PRI_INFO  , arg)
#define QLOG_DEBUG_S(marker, arg)  QLOG_LOG_S(marker, NActors::NLog::PRI_DEBUG , arg)

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

namespace NKikimr::NBsQueue {

// Special timer for debug purposes, which works with virtual time of TTestActorSystem
struct TActivationContextTimer {
    TActivationContextTimer()
        : CreationTimestamp(NActors::TActivationContext::Monotonic())
    {}

    double Passed() const {
        return (NActors::TActivationContext::Monotonic() - CreationTimestamp).SecondsFloat();
    }

    TMonotonic CreationTimestamp;
};

struct TBSQueueTimer {
    TBSQueueTimer(bool useActorSystemTime)
    {
        if (useActorSystemTime) {
            Timer.emplace<TActivationContextTimer>();
        } else {
            Timer.emplace<THPTimer>();
        }
    }

    std::variant<THPTimer, TActivationContextTimer> Timer;

    double Passed() const {
        return std::visit([](const auto& timer) -> double {
            return timer.Passed();
        }, Timer);
    }
};

} // namespace NKikimr::NBsQueue