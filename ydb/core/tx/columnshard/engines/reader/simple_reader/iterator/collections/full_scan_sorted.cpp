#include "full_scan_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

bool TSortedFullScanCollection::DoCheckInFlightLimits() const {
    // Check both sources and pages limits
    if (GetSourcesInFlightCount() >= GetMaxInFlight()) {
        return false;
    }
    // If MaxPagesInFlight is set (non-zero), also check pages limit
    if (GetMaxPagesInFlight() > 0 && GetPagesInFlightCount() >= GetMaxPagesInFlight()) {
        return false;
    }
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
