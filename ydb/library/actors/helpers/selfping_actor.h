#pragma once

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {

ui64 MeasureTaskDurationNs();
NActors::IActor* CreateSelfPingActor(
    TDuration sendInterval,
    const NMonitoring::TDynamicCounters::TCounterPtr& maxPingCounter,
    const NMonitoring::TDynamicCounters::TCounterPtr& avgPingCounter,
    const NMonitoring::TDynamicCounters::TCounterPtr& avgPingSmallWindowCounter,
    const NMonitoring::TDynamicCounters::TCounterPtr& calculationTimeCounter);

} // NActors
