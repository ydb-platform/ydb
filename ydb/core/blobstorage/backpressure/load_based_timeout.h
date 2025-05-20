#pragma once

#include "defs.h"
#include <algorithm>
#include <atomic>

namespace NKikimr {

// We assume only one active request can be in flight
class TLoadBasedTimeoutManager {
public:
    TLoadBasedTimeoutManager(TDuration minimumTimeout, TDuration timePerRequestInFlight);
    ~TLoadBasedTimeoutManager();

    TDuration GetTimeoutForNewRequest();
    void RequestCompleted();

private:
    TDuration MinimumTimeout;
    TDuration TimePerRequestInFlight;
    static std::atomic<ui32> RequestsInFlight;
    bool ActiveRequest;
};

} // namespace NKikimr
