#pragma once

#include "defs.h"

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
