#include "spilling_counters.h"

namespace NYql::NDq {

TSpillingCounters::TSpillingCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    SpillingWriteBlobs = counters->GetCounter("Spilling/WriteBlobs", true);
    SpillingReadBlobs = counters->GetCounter("Spilling/ReadBlobs", true);
    SpillingStoredBlobs = counters->GetCounter("Spilling/StoredBlobs", false);
    SpillingTotalSpaceUsed = counters->GetCounter("Spilling/TotalSpaceUsed", false);
    SpillingTooBigFileErrors = counters->GetCounter("Spilling/TooBigFileErrors", true);
    SpillingNoSpaceErrors = counters->GetCounter("Spilling/NoSpaceErrors", true);
    SpillingIoErrors = counters->GetCounter("Spilling/IoErrors", true);
}

} // namespace NYql::NDq
