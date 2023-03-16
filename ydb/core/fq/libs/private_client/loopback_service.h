#pragma once

#include "events.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

NActors::IActor* CreateLoopbackServiceActor(
    const ::NMonitoring::TDynamicCounterPtr& counters);

} /* NFq */
