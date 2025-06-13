#include "counters.h"

namespace NYql::NDqs {

TWorkerManagerCounters::TWorkerManagerCounters(
    ::NMonitoring::TDynamicCounterPtr root,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    ::NMonitoring::TDynamicCounterPtr userCounters)
   : ActiveWorkers(root->GetCounter("ActiveWorkers"))
   , MkqlMemoryLimit(root->GetCounter("MkqlMemoryLimit"))
   , MkqlMemoryAllocated(root->GetCounter("MkqlMemoryAllocated"))
   , FreeGroupError(root->GetCounter("FreeGroupError"))
   , ElapsedMicrosec(userCounters->GetCounter("Scheduler/ElapsedMicrosec"))
   , TaskCounters(taskCounters)
{ }

TWorkerManagerCounters::TWorkerManagerCounters(::NMonitoring::TDynamicCounterPtr root)
    : TWorkerManagerCounters(
        root->GetSubgroup("component", "lwm"),
        root->GetSubgroup("component", "tasks"),
        root->GetSubgroup("counters", "utils")->GetSubgroup("execpool", "User"))
{ }

TWorkerManagerCounters::TWorkerManagerCounters()
    : TWorkerManagerCounters(new ::NMonitoring::TDynamicCounters)
{ }

} // namespace NYql::NDqs
