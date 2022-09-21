#pragma once

#include "events.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYq {

NActors::IActor* CreateLoopbackServiceActor(
    const ::NMonitoring::TDynamicCounterPtr& counters);

} /* NYq */
