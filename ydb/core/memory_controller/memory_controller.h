#pragma once


#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/mon_alloc/memory_info.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NMemory {

NActors::IActor* CreateMemoryController(
    TDuration interval,
    TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
    const NKikimrConfig::TMemoryControllerConfig& config, 
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

}