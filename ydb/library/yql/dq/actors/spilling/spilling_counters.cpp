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

TSpillingCountersPerTaskRunner::TSpillingCountersPerTaskRunner(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 taskId) 
    : Counters(counters), TaskId(taskId) {
    TString prefix = "Spilling/TaskRunner-" + ToString(TaskId) + "/";
    SpillingReadBytes = Counters->GetCounter(prefix + "ReadBytes", true);
    SpillingWriteBytes = Counters->GetCounter(prefix + "WriteBytes", true);
}

TSpillingCountersPerTaskRunner::~TSpillingCountersPerTaskRunner() {
    TString prefix = "Spilling/TaskRunner-" + ToString(TaskId) + "/";
    Counters->RemoveCounter(prefix + "ReadBytes");
    Counters->RemoveCounter(prefix + "WriteBytes");
}

} // namespace NYql::NDq
