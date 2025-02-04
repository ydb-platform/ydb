#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NEtcd {

NActors::IActor* CreateEtcdLeasingOffice(TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

}
