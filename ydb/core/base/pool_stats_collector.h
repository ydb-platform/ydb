#pragma once
#include "defs.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    struct TActorSystemSetup;

}

namespace NKikimr {

    IActor* CreateStatsCollector(ui32 intervalSec,
                             const TActorSystemSetup& setup,
                             ::NMonitoring::TDynamicCounterPtr counters);

}
