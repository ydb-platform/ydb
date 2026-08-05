#include "hang_tracker.h"

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

THangTracker::THangTracker(const TDuration timeout)
    : Timeout(timeout)
    , CheckInterval(Max(timeout / 10, TDuration::MilliSeconds(50)))
{
}

std::optional<TDuration> THangTracker::OnProgress(const TMonotonic now) {
    LastProgressInstant = now;
    if (Scheduled) {
        return std::nullopt;
    }
    Scheduled = true;
    return CheckInterval;
}

THangTracker::TWakeupResult THangTracker::OnWakeup(const bool isActive, const TMonotonic now) {
    Scheduled = false;
    TWakeupResult result;
    if (isActive && LastProgressInstant && (now - *LastProgressInstant) > Timeout) {
        result.TimedOut = true;
        return result;
    }
    if (isActive) {
        Scheduled = true;
        result.RescheduleAfter = CheckInterval;
    }
    return result;
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
