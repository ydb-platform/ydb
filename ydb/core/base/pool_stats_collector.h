#pragma once
#include "defs.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    struct TActorSystemSetup;

}

namespace NKikimr {

    IActor* CreateStatsCollector(ui32 intervalSec,
                             const TActorSystemSetup& setup,
                             ::NMonitoring::TDynamicCounterPtr counters);

}
