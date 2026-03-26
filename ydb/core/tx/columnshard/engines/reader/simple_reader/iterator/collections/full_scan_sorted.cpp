#include "full_scan_sorted.h"

namespace NKikimr::NOlap::NReader::NSimple {

bool TSortedFullScanCollection::DoCheckInFlightLimits() const {
    // Check both sources and pages limits
    if (GetSourcesInFlightCount() >= GetMaxInFlight()) {
        return false;
    }
    if (GetUsePagesInFlightLimit() && GetPagesInFlightCount() >= GetMaxPagesInFlight()) {
        return false;
    }
    return true;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
