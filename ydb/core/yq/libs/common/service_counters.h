#pragma once

#include <util/generic/string.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYq {
namespace NCommon {

struct TServiceCounters {
    NMonitoring::TDynamicCounterPtr RootCounters;   // "counters/counters=yq" - root counters for service metrics
    NMonitoring::TDynamicCounterPtr PublicCounters; // "counters/counters=public" - root counters for cloud user metrics
    NMonitoring::TDynamicCounterPtr Counters;       // "counters/counters=yq/subsystem=smth" - subsystem part, may match to RootCounters if subsystem name is empty

    explicit TServiceCounters(const NMonitoring::TDynamicCounterPtr& rootCounters, const NMonitoring::TDynamicCounterPtr& publicCounters, const TString& subsystemName = "")
        : RootCounters(rootCounters)
        , PublicCounters(publicCounters)
        , Counters(subsystemName ? RootCounters->GetSubgroup("subsystem", subsystemName) : RootCounters)
    {
    }

    explicit TServiceCounters(const NMonitoring::TDynamicCounterPtr& baseCounters, const TString& subsystemName = "")
        : RootCounters(baseCounters->GetSubgroup("counters", "yq"))
        , PublicCounters(baseCounters->GetSubgroup("counters", "yq_public"))
        , Counters(subsystemName ? RootCounters->GetSubgroup("subsystem", subsystemName) : RootCounters)
    {
    }

    explicit TServiceCounters(const TServiceCounters& serviceCounters, const TString& subsystemName = "")
        : RootCounters(serviceCounters.RootCounters)
        , PublicCounters(serviceCounters.PublicCounters)
        , Counters(subsystemName ? RootCounters->GetSubgroup("subsystem", subsystemName) : serviceCounters.Counters)
    {
    }
 
    TServiceCounters& operator=(const TServiceCounters& serviceCounters) = default; 
};

} // namespace NCommon
} // namespace NYq
