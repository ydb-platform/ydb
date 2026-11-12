#include "common_data.h"

namespace NKikimr::NColumnShard {

TLoadTimeSignals::TLoadTimer::~TLoadTimer() {
    const TDuration duration = TMonotonic::Now() - Start;
    if (Failed) {
        Signals.AddFailedLoadingTime(duration.MicroSeconds());
    } else {
        Signals.AddLoadingTime(duration.MicroSeconds());
    }
    if (Total) {
        *Total += duration;
    }
}

void TLoadTimeSignals::TLoadTimer::AddLoadingFail() {
    if (!Failed) {
        Failed = true;
        Signals.AddLoadingFail();
    }
}

}   // namespace NKikimr::NColumnShard
