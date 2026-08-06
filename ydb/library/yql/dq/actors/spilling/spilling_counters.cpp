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

static void InitDDiskCounters(TSpillingCounters::TDDiskCounters& dc,
                              const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    const TString prefix = "Spilling/DDisk";
    dc.ActiveSessions = counters->GetCounter(prefix + "/ActiveSessions", false);
    dc.Discoveries = counters->GetCounter(prefix + "/Discoveries", true);
    dc.DiscoveryErrors = counters->GetCounter(prefix + "/DiscoveryErrors", true);
    dc.Connects = counters->GetCounter(prefix + "/Connects", true);
    dc.ConnectErrors = counters->GetCounter(prefix + "/ConnectErrors", true);
    dc.WriteBytes = counters->GetCounter(prefix + "/WriteBytes", true);
    dc.ReadBytes = counters->GetCounter(prefix + "/ReadBytes", true);
    dc.WriteParts = counters->GetCounter(prefix + "/WriteParts", true);
    dc.ReadParts = counters->GetCounter(prefix + "/ReadParts", true);
    dc.Erases = counters->GetCounter(prefix + "/Erases", true);
    dc.InFlightWrites = counters->GetCounter(prefix + "/InFlightWrites", false);
    dc.InFlightReads = counters->GetCounter(prefix + "/InFlightReads", false);
}

TSpillingCounters::TSpillingCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    InitTypeCounters(ComputeSpilling, counters, "Spilling/Compute");
    InitTypeCounters(ChannelSpilling, counters, "Spilling/Channel");
    InitDDiskCounters(DDisk, counters);
    SpillingIOQueueSize = counters->GetCounter("Spilling/IOQueueSize", false);
}

} // namespace NYql::NDq
