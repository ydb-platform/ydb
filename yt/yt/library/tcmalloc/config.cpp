#include "config.h"

namespace NYT::NTCMalloc {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void THeapSizeLimitConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("container_memory_ratio", &TThis::ContainerMemoryRatio)
        .Optional();
    registrar.Parameter("container_memory_margin", &TThis::ContainerMemoryMargin)
        .Optional();
    registrar.Parameter("hard", &TThis::Hard)
        .Default(false);
    registrar.Parameter("dump_memory_profile_on_violation", &TThis::DumpMemoryProfileOnViolation)
        .Default(false);
    registrar.Parameter("dump_memory_profile_timeout", &TThis::DumpMemoryProfileTimeout)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("dump_memory_profile_path", &TThis::DumpMemoryProfilePath)
        .Default();
}

TTCMallocConfigPtr TTCMallocConfig::ApplyDynamic(const TTCMallocConfigPtr& dynamicConfig) const
{
    // TODO(babenko): fix this mess
    auto mergedConfig = CloneYsonStruct(dynamicConfig);
    mergedConfig->HeapSizeLimit->DumpMemoryProfilePath = HeapSizeLimit->DumpMemoryProfilePath;
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TTCMallocConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("background_release_rate", &TThis::BackgroundReleaseRate)
        .Default(32_MB);
    registrar.Parameter("max_per_cpu_cache_size", &TThis::MaxPerCpuCacheSize)
        .Default(3_MB);

    registrar.Parameter("aggressive_release_threshold", &TThis::AggressiveReleaseThreshold)
        .Default(20_GB);
    registrar.Parameter("aggressive_release_threshold_ratio", &TThis::AggressiveReleaseThresholdRatio)
        .Optional();

    registrar.Parameter("aggressive_release_size", &TThis::AggressiveReleaseSize)
        .Default(128_MB);
    registrar.Parameter("aggressive_release_period", &TThis::AggressiveReleasePeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("guarded_sampling_rate", &TThis::GuardedSamplingRate)
        .Default(128_MB);

    registrar.Parameter("heap_size_limit", &TThis::HeapSizeLimit)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc

