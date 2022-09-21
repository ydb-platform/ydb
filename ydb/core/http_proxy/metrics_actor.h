#pragma once

#include "events.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NHttpProxy {

    struct TMetricsSettings {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
        //TODO: TTL
    };

    NActors::IActor* CreateMetricsActor(const TMetricsSettings& settings);

} // namespace
