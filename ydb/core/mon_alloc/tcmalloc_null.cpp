#include "tcmalloc.h" 
 
namespace NKikimr { 
 
std::unique_ptr<IAllocStats> CreateTcMallocStats( 
    TIntrusivePtr<NMonitoring::TDynamicCounters> group) 
{ 
    Y_UNUSED(group); 
    return {}; 
} 
 
std::unique_ptr<IAllocState> CreateTcMallocState() { 
    return {}; 
} 
 
std::unique_ptr<IAllocMonitor> CreateTcMallocMonitor( 
    TIntrusivePtr<NMonitoring::TDynamicCounters> group) 
{ 
    Y_UNUSED(group); 
    return {}; 
} 
 
std::unique_ptr<NActors::IProfilerLogic> CreateTcMallocProfiler() { 
    return {}; 
} 
 
} 
