#pragma once

#include "etcd_shared.h"
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NEtcd {

NActors::IActor* BuildWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff);

}

