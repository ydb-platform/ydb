#include "counters.h"

namespace NYql::NDqs {

TWorkerManagerCounters::TWorkerManagerCounters(
    ::NMonitoring::TDynamicCounterPtr root,
    ::NMonitoring::TDynamicCounterPtr taskCounters)
   : ActiveWorkers(root->GetCounter("ActiveWorkers"))
   , MkqlMemoryLimit(root->GetCounter("MkqlMemoryLimit"))
   , MkqlMemoryAllocated(root->GetCounter("MkqlMemoryAllocated"))
   , FreeGroupError(root->GetCounter("FreeGroupError"))
   , TaskCounters(taskCounters)
{ }

TWorkerManagerCounters::TWorkerManagerCounters(::NMonitoring::TDynamicCounterPtr root)
    : TWorkerManagerCounters(
        root->GetSubgroup("component", "lwm"),
        root->GetSubgroup("component", "tasks"))
{ }

TWorkerManagerCounters::TWorkerManagerCounters()
    : TWorkerManagerCounters(new ::NMonitoring::TDynamicCounters)
{ }

} // namespace NYql::NDqs
