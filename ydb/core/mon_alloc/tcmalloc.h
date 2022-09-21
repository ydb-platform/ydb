#pragma once

#include "monitor.h"
#include "profiler.h"
#include "stats.h"

namespace NKikimr {

std::unique_ptr<IAllocStats> CreateTcMallocStats(
    TIntrusivePtr<::NMonitoring::TDynamicCounters> group);

std::unique_ptr<IAllocState> CreateTcMallocState();

std::unique_ptr<IAllocMonitor> CreateTcMallocMonitor(
    TIntrusivePtr<::NMonitoring::TDynamicCounters> group);

std::unique_ptr<NActors::IProfilerLogic> CreateTcMallocProfiler();


}
