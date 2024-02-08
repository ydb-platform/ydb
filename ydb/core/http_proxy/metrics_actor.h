#pragma once

#include "events.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NHttpProxy {

    struct TMetricsSettings {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
        //TODO: TTL
    };

    NActors::IActor* CreateMetricsActor(const TMetricsSettings& settings);

} // namespace
