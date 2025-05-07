#include "defs.h"
#include <algorithm>
#include <atomic>

namespace NKikimr {

// We assume only one active request can be in flight
class TLoadBasedTimeoutManager {
public:
    TLoadBasedTimeoutManager(TDuration minimumTimeout, TDuration timePerRequestInFlight)
        : MinimumTimeout(minimumTimeout)
        , TimePerRequestInFlight(timePerRequestInFlight)
        , ActiveRequest(false)
    {}

    TDuration GetTimeoutForNewRequest() {
        // if there is active request we don't increment counter
        ui32 delta = std::exchange(ActiveRequest, true) ? 0 : 1;
        ui32 requestsInFlight = RequestsInFlight.fetch_add(delta) + delta;
        return std::max(MinimumTimeout, TimePerRequestInFlight * requestsInFlight);
    }

    void RequestCompleted() {
        if (std::exchange(ActiveRequest, false)) {
            ui32 prev = RequestsInFlight.fetch_sub(1);
            Y_DEBUG_ABORT_UNLESS(prev > 0);
        }
    }

    ~TLoadBasedTimeoutManager() {
        RequestCompleted();
    }

private:
    TDuration MinimumTimeout;
    TDuration TimePerRequestInFlight;
    static std::atomic<ui32> RequestsInFlight;
    bool ActiveRequest;
};

std::atomic<ui32> TLoadBasedTimeoutManager::RequestsInFlight = 0;

} // namespace NKikimr
