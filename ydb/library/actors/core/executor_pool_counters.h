#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/cast.h>

#include <optional>

namespace NActors {

inline NMonitoring::TDynamicCounterPtr GetExecutorPoolCountersGroup(
        NMonitoring::TDynamicCounters* counters,
        const TString& poolName,
        const std::optional<ui32> placementGroupId) {
    auto poolGroup = counters->GetSubgroup("execpool", poolName);
    if (placementGroupId) {
        poolGroup = poolGroup->GetSubgroup("placement", ToString(*placementGroupId));
    }
    return poolGroup;
}

inline NMonitoring::TDynamicCounterPtr FindExecutorPoolCountersGroup(
        const NMonitoring::TDynamicCounters* counters,
        const TString& poolName,
        const std::optional<ui32> placementGroupId) {
    auto poolGroup = counters->FindSubgroup("execpool", poolName);
    if (poolGroup && placementGroupId) {
        poolGroup = poolGroup->FindSubgroup("placement", ToString(*placementGroupId));
    }
    return poolGroup;
}

} // namespace NActors
