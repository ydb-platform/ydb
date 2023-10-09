#include "service_counters.h"

namespace NYql {
namespace NCommon {

TServiceCounters::TServiceCounters(
    const ::NMonitoring::TDynamicCounterPtr& rootCounters,
    const ::NMonitoring::TDynamicCounterPtr& publicCounters,
    const TString& subsystemName)
    : RootCounters(rootCounters)
    , PublicCounters(publicCounters)
    , Counters(subsystemName ? RootCounters->GetSubgroup("subsystem", subsystemName) : RootCounters)
{
}

TServiceCounters::TServiceCounters(
    const ::NMonitoring::TDynamicCounterPtr& baseCounters,
    const TString& subsystemName)
    : RootCounters(baseCounters->GetSubgroup("counters", "yq"))
    , PublicCounters(baseCounters->GetSubgroup("counters", "yq_public"))
    , Counters(subsystemName ? RootCounters->GetSubgroup("subsystem", subsystemName) : RootCounters)
{
}

TServiceCounters::TServiceCounters(
    const TServiceCounters& serviceCounters,
    const TString& subsystemName)
    : RootCounters(serviceCounters.RootCounters)
    , PublicCounters(serviceCounters.PublicCounters)
    , Counters(subsystemName ? RootCounters->GetSubgroup("subsystem", subsystemName) : serviceCounters.Counters)
    , UptimeRootCounter(serviceCounters.UptimeRootCounter)
    , UptimePublicCounter(serviceCounters.UptimePublicCounter)
{
}

void TServiceCounters::InitUptimeCounter() {
    UptimePublicCounter = PublicCounters->GetNamedCounter("name", "query.uptime_seconds", false);
    UptimeRootCounter = RootCounters->GetNamedCounter("sensor", "UptimeSeconds", false);
}

void TServiceCounters::SetUptimePublicAndServiceCounter(i64 val) const {
    Y_ABORT_UNLESS(UptimePublicCounter && UptimeRootCounter);
    UptimePublicCounter->Set(val);
    UptimeRootCounter->Set(val);
}

} // namespace NCommon
} // namespace NYql
