#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    struct TActorSystemSetup;
}

// copy from kikimr/core/base/pool_stats_collector.h

namespace NYql {

    NActors::IActor* CreateStatsCollector(ui32 intervalSec,
                             const NActors::TActorSystemSetup& setup,
                             NMonitoring::TDynamicCounterPtr counters);

}
