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

struct TBSQueueTimer {
    const bool UseActorSystemTime;
    ui64 Timestamp;

    TBSQueueTimer(bool useActorSystemTime)
        : UseActorSystemTime(useActorSystemTime)
    {
        if (useActorSystemTime) {
            Timestamp = NActors::TActivationContext::Monotonic().GetValue();
        } else {
            NHPTimer::STime start;
            NHPTimer::GetTime(&start);
            Timestamp = static_cast<ui64>(start);
        }
    }

    double Passed() const {
        if (UseActorSystemTime) {
            return (NActors::TActivationContext::Monotonic() - TMonotonic::FromValue(Timestamp)).SecondsFloat();
        }
        NHPTimer::STime start = static_cast<NHPTimer::STime>(Timestamp);
        return NHPTimer::GetTimePassed(&start);
    }
};

} // namespace NKikimr::NBsQueue
