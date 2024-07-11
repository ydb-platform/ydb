#pragma once


#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NMemory {

TIntrusivePtr<IMemoryConsumers> CreateMemoryConsumers();

NActors::IActor* CreateMemoryController(TDuration interval, TIntrusivePtr<IMemoryConsumers> consumers, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

}