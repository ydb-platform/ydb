#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYql::NDqs {

struct TWorkerManagerCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr ActiveWorkers;
    ::NMonitoring::TDynamicCounters::TCounterPtr MkqlMemoryLimit;
    ::NMonitoring::TDynamicCounters::TCounterPtr MkqlMemoryAllocated;
    ::NMonitoring::TDynamicCounters::TCounterPtr FreeGroupError;
    ::NMonitoring::TDynamicCounterPtr TaskCounters;

    explicit TWorkerManagerCounters(::NMonitoring::TDynamicCounterPtr root);
    explicit TWorkerManagerCounters(::NMonitoring::TDynamicCounterPtr root, ::NMonitoring::TDynamicCounterPtr taskCounters);
    TWorkerManagerCounters();
};

} // namespace NYql::NDqs
