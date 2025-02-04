#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NEtcd {

NActors::IActor* CreateEtcdWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

}

