#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NEtcd {

NActors::IActor* BuildWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

}

