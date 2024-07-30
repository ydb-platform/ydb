#pragma once


#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NMemory {

struct TProcessMemoryInfo {
    ui64 AllocatedMemory;
    std::optional<ui64> AnonRss;
    std::optional<ui64> CGroupLimit;
};

struct IProcessMemoryInfoProvider : public TThrRefBase {
    virtual TProcessMemoryInfo Get() const = 0;
};

struct TProcessMemoryInfoProvider : public IProcessMemoryInfoProvider {
    TProcessMemoryInfo Get() const override;
};

NActors::IActor* CreateMemoryController(
    TDuration interval,
    TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
    const NKikimrConfig::TMemoryControllerConfig& config, 
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters);

}