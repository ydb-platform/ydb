#include "control_plane_storage_counters.h"

namespace NFq {

TRequestScopeCounters::TRequestScopeCounters(const TString& name)
    : Name(name) {}

void TRequestScopeCounters::Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
    Counters = counters;
    auto requestCounters = counters->GetSubgroup("request_scope", Name);
    InFly = requestCounters->GetCounter("InFly", false);
    Ok = requestCounters->GetCounter("Ok", true);
    Error = requestCounters->GetCounter("Error", true);
    Retry = requestCounters->GetCounter("Retry", true);
}

TRequestScopeCounters::~TRequestScopeCounters() {
    Counters->RemoveSubgroup("request_scope", Name);
}

TRequestCommonCounters::TRequestCommonCounters(const TString& name)
    : Name(name) {}

void TRequestCommonCounters::Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
    Counters = counters;
    auto requestCounters = counters->GetSubgroup("request_common", Name);
    InFly = requestCounters->GetCounter("InFly", false);
    Ok = requestCounters->GetCounter("Ok", true);
    Error = requestCounters->GetCounter("Error", true);
    ParseProtobufError = requestCounters->GetCounter("ParseProtobufError", true);
    Retry = requestCounters->GetCounter("Retry", true);
    RequestBytes = requestCounters->GetCounter("RequestBytes", true);
    ResponseBytes = requestCounters->GetCounter("ResponseBytes", true);
    LatencyMs = requestCounters->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
    Issues = requestCounters->GetSubgroup("subcomponent", "Issues");
}

NMonitoring::IHistogramCollectorPtr TRequestCommonCounters::GetLatencyHistogramBuckets() {
    return NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
}

TRequestCommonCounters::~TRequestCommonCounters() {
    Counters->RemoveSubgroup("request_common", Name);
}

TFinalStatusCounters::TFinalStatusCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
    : Counters(counters) {
    auto subgroup = counters->GetSubgroup("subcomponent", "FinalStatus");
    Completed = subgroup->GetCounter("COMPLETED", true);
    AbortedBySystem = subgroup->GetCounter("ABORTED_BY_SYSTEM", true);
    AbortedByUser = subgroup->GetCounter("ABORTED_BY_USER", true);
    Failed = subgroup->GetCounter("FAILED", true);
    Paused = subgroup->GetCounter("PAUSED", true);

    auto subgroupReason = counters->GetSubgroup("subcomponent", "FinalReason");
    Unavailable = subgroup->GetCounter("Unavailable", true);
}

void TFinalStatusCounters::IncByStatus(FederatedQuery::QueryMeta::ComputeStatus finalStatus) {
    switch (finalStatus) {
    case FederatedQuery::QueryMeta::COMPLETED:
        Completed->Inc();
        break;
    case FederatedQuery::QueryMeta::FAILED:
        Failed->Inc();
        break;
    case FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM:
        AbortedBySystem->Inc();
        break;
    case FederatedQuery::QueryMeta::ABORTED_BY_USER:
        AbortedByUser->Inc();
        break;
    case FederatedQuery::QueryMeta::PAUSED:
        Paused->Inc();
        break;
    case FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED:
    case FederatedQuery::QueryMeta::STARTING:
    case FederatedQuery::QueryMeta::ABORTING_BY_USER:
    case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
    case FederatedQuery::QueryMeta::RESUMING:
    case FederatedQuery::QueryMeta::RUNNING:
    case FederatedQuery::QueryMeta::COMPLETING:
    case FederatedQuery::QueryMeta::FAILING:
    case FederatedQuery::QueryMeta::PAUSING:
        break;
    default:
        Y_ENSURE(true, "Unexpected status: " << FederatedQuery::QueryMeta_ComputeStatus_Name(finalStatus));
    }
}

TFinalStatusCounters::~TFinalStatusCounters() {
    Counters->RemoveSubgroup("subcomponent", "FinalStatus");
}

} // NFq
