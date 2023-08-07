#include "config.h"

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

void TYTAllocConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_allocation_profiling", &TThis::EnableAllocationProfiling)
        .Default();
    registrar.Parameter("allocation_profiling_sampling_rate", &TThis::AllocationProfilingSamplingRate)
        .InRange(0.0, 1.0)
        .Default();
    registrar.Parameter("small_arenas_to_profile", &TThis::SmallArenasToProfile)
        .Default();
    registrar.Parameter("large_arenas_to_profile", &TThis::LargeArenasToProfile)
        .Default();
    registrar.Parameter("profiling_backtrace_depth", &TThis::ProfilingBacktraceDepth)
        .InRange(1, MaxAllocationProfilingBacktraceDepth)
        .Default();
    registrar.Parameter("min_profiling_bytes_used_to_report", &TThis::MinProfilingBytesUsedToReport)
        .GreaterThan(0)
        .Default();
    registrar.Parameter("stockpile_interval", &TThis::StockpileInterval)
        .Default();
    registrar.Parameter("stockpile_thread_count", &TThis::StockpileThreadCount)
        .Default();
    registrar.Parameter("stockpile_size", &TThis::StockpileSize)
        .GreaterThan(0)
        .Default();
    registrar.Parameter("enable_eager_memory_release", &TThis::EnableEagerMemoryRelease)
        .Default();
    registrar.Parameter("enable_madvise_populate", &TThis::EnableMadvisePopulate)
        .Default();
    registrar.Parameter("large_unreclaimable_coeff", &TThis::LargeUnreclaimableCoeff)
        .Default();
    registrar.Parameter("min_large_unreclaimable_bytes", &TThis::MinLargeUnreclaimableBytes)
        .Default();
    registrar.Parameter("max_large_unreclaimable_bytes", &TThis::MaxLargeUnreclaimableBytes)
        .Default();
}


void TYTProfilingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_detailed_allocation_statistics", &TThis::EnableDetailedAllocationStatistics)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
