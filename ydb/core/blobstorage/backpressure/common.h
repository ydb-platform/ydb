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

// Measures elapsed time either with the HP timer or, for debug purposes, with the virtual time of
// TTestActorSystem. The choice is a per-queue constant, so instead of a std::variant of two timers
// (which makes every Passed() call an indirect dispatch) keep the flag next to the raw timestamp --
// both alternatives are a single 8-byte value anyway, so this costs no extra space.
struct TBSQueueTimer {
    TBSQueueTimer(bool useActorSystemTime)
        : UseActorSystemTime(useActorSystemTime)
    {
        if (Y_UNLIKELY(useActorSystemTime)) {
            Timestamp = NActors::TActivationContext::Monotonic().GetValue();
        } else {
            NHPTimer::STime start;
            NHPTimer::GetTime(&start);
            Timestamp = static_cast<ui64>(start);
        }
    }

    double Passed() const {
        if (Y_UNLIKELY(UseActorSystemTime)) {
            return (NActors::TActivationContext::Monotonic() - TMonotonic::FromValue(Timestamp)).SecondsFloat();
        }
        // GetTimePassed() mutates its argument, so feed it a copy
        NHPTimer::STime start = static_cast<NHPTimer::STime>(Timestamp);
        return NHPTimer::GetTimePassed(&start);
    }

    bool UseActorSystemTime;
    ui64 Timestamp;
};

} // namespace NKikimr::NBsQueue
