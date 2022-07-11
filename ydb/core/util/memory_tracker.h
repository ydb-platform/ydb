#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    class IActor;
}

namespace NKikimr {
namespace NMemory {

NActors::IActor* CreateMemoryTrackerActor(
    TDuration updateInterval,
    ::NMonitoring::TDynamicCounterPtr counters);

}
}
