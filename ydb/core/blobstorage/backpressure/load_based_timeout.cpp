#include "load_based_timeout.h"

namespace NKikimr {

TLoadBasedTimeoutManager::TLoadBasedTimeoutManager(TDuration minimumTimeout, TDuration timePerRequestInFlight)
    : MinimumTimeout(minimumTimeout)
    , TimePerRequestInFlight(timePerRequestInFlight)
    , ActiveRequest(false)
{}

TDuration TLoadBasedTimeoutManager::GetTimeoutForNewRequest() {
    // if there is active request we don't increment counter
    ui32 delta = std::exchange(ActiveRequest, true) ? 0 : 1;
    ui32 requestsInFlight = RequestsInFlight.fetch_add(delta) + delta;
    return std::max(MinimumTimeout, TimePerRequestInFlight * requestsInFlight);
}

void TLoadBasedTimeoutManager::RequestCompleted() {
    if (std::exchange(ActiveRequest, false)) {
        ui32 prev = RequestsInFlight.fetch_sub(1);
        Y_DEBUG_ABORT_UNLESS(prev > 0);
    }
}


TLoadBasedTimeoutManager::~TLoadBasedTimeoutManager() {
    RequestCompleted();
}

std::atomic<ui32> TLoadBasedTimeoutManager::RequestsInFlight = 0;

} // namespace NKikimr
