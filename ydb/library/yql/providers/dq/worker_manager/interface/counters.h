#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql::NDqs {

struct TWorkerManagerCounters {
    NMonitoring::TDynamicCounters::TCounterPtr ActiveWorkers;
    NMonitoring::TDynamicCounters::TCounterPtr MkqlMemoryLimit;
    NMonitoring::TDynamicCounters::TCounterPtr MkqlMemoryAllocated;
    NMonitoring::TDynamicCounters::TCounterPtr FreeGroupError;

    explicit TWorkerManagerCounters(NMonitoring::TDynamicCounterPtr root);
    TWorkerManagerCounters();
};

} // namespace NYql::NDqs
