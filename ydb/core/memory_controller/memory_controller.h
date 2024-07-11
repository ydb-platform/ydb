#pragma once


#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NMemory {

NActors::IActor* CreateMemoryController(TDuration interval, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

}