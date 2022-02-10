#include "counters.h" 
 
namespace NYql::NDqs { 
 
TWorkerManagerCounters::TWorkerManagerCounters(NMonitoring::TDynamicCounterPtr root) { 
   ActiveWorkers = root->GetCounter("ActiveWorkers"); 
   MkqlMemoryLimit = root->GetCounter("MkqlMemoryLimit");
   MkqlMemoryAllocated = root->GetCounter("MkqlMemoryAllocated");
} 
 
TWorkerManagerCounters::TWorkerManagerCounters()
    : TWorkerManagerCounters(new NMonitoring::TDynamicCounters)
{ }

} // namespace NYql::NDqs
