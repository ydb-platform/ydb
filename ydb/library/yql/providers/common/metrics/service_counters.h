#pragma once

#include <util/generic/string.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql {
namespace NCommon {

struct TServiceCounters {
    ::NMonitoring::TDynamicCounterPtr RootCounters;   // "counters/counters=yq" - root counters for service metrics
    ::NMonitoring::TDynamicCounterPtr PublicCounters; // "counters/counters=public" - root counters for cloud user metrics
    ::NMonitoring::TDynamicCounterPtr Counters;       // "counters/counters=yq/subsystem=smth" - subsystem part, may match to RootCounters if subsystem name is empty

    ::NMonitoring::TDynamicCounters::TCounterPtr UptimeRootCounter; // yq/uptime_seconds
    ::NMonitoring::TDynamicCounters::TCounterPtr UptimePublicCounter; // yq_public/query.uptime_seconds

    explicit TServiceCounters(
        const ::NMonitoring::TDynamicCounterPtr& rootCounters,
        const ::NMonitoring::TDynamicCounterPtr& publicCounters,
        const TString& subsystemName = "");

    explicit TServiceCounters(
        const ::NMonitoring::TDynamicCounterPtr& baseCounters,
        const TString& subsystemName = "");

    explicit TServiceCounters(
        const TServiceCounters& serviceCounters,
        const TString& subsystemName = "");

    TServiceCounters& operator=(const TServiceCounters& serviceCounters) = default;

    void InitUptimeCounter();

    void SetUptimePublicAndServiceCounter(i64 val) const;
};

} // namespace NCommon
} // namespace NYql
