#include "spilling_counters.h"

namespace NYql::NDq {

static void InitTypeCounters(TSpillingCounters::TTypeCounters& tc,
                             const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                             const TString& prefix) {
    tc.WriteBlobs = counters->GetCounter(prefix + "/WriteBlobs", true);
    tc.ReadBlobs = counters->GetCounter(prefix + "/ReadBlobs", true);
    tc.StoredBlobs = counters->GetCounter(prefix + "/StoredBlobs", false);
    tc.TotalSpaceUsed = counters->GetCounter(prefix + "/TotalSpaceUsed", false);
    tc.TooBigFileErrors = counters->GetCounter(prefix + "/TooBigFileErrors", true);
    tc.NoSpaceErrors = counters->GetCounter(prefix + "/NoSpaceErrors", true);
    tc.IoErrors = counters->GetCounter(prefix + "/IoErrors", true);
    tc.FileDescriptors = counters->GetCounter(prefix + "/FileDescriptors", false);
}

TSpillingCounters::TSpillingCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    InitTypeCounters(ComputeSpilling, counters, "Spilling/Compute");
    InitTypeCounters(ChannelSpilling, counters, "Spilling/Channel");
    SpillingIOQueueSize = counters->GetCounter("Spilling/IOQueueSize", false);
}

} // namespace NYql::NDq
