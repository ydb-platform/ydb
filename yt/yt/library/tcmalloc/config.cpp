#include "config.h"

namespace NYT::NTCMalloc {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void THeapSizeLimitConfig::ApplyDynamicInplace(const TDynamicHeapSizeLimitConfigPtr& dynamicConfig)
{
    UpdateYsonStructField(ContainerMemoryRatio, dynamicConfig->ContainerMemoryRatio);
    UpdateYsonStructField(ContainerMemoryMargin, dynamicConfig->ContainerMemoryMargin);
    UpdateYsonStructField(Hard, dynamicConfig->Hard);
    UpdateYsonStructField(DumpMemoryProfileOnViolation, dynamicConfig->DumpMemoryProfileOnViolation);
    UpdateYsonStructField(MemoryProfileDumpTimeout, dynamicConfig->MemoryProfileDumpTimeout);
    UpdateYsonStructField(MemoryProfileDumpPath, dynamicConfig->MemoryProfileDumpPath);
    UpdateYsonStructField(MemoryProfileDumpFilenameSuffix, dynamicConfig->MemoryProfileDumpFilenameSuffix);
}

THeapSizeLimitConfigPtr THeapSizeLimitConfig::ApplyDynamic(const TDynamicHeapSizeLimitConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    mergedConfig->ApplyDynamicInplace(dynamicConfig);
    mergedConfig->Postprocess();
    return mergedConfig;
}

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
    registrar.Parameter("memory_profile_dump_timeout", &TThis::MemoryProfileDumpTimeout)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("memory_profile_dump_path", &TThis::MemoryProfileDumpPath)
        .Default();
    registrar.Parameter("memory_profile_dump_filename_suffix", &TThis::MemoryProfileDumpFilenameSuffix)
        .Default();

    registrar.Postprocessor([] (THeapSizeLimitConfig* config) {
        if (config->DumpMemoryProfileOnViolation && !config->MemoryProfileDumpPath) {
            THROW_ERROR_EXCEPTION("\"memory_profile_dump_path\" must be set when \"dump_memory_profile_on_violation\" is true");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicHeapSizeLimitConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("container_memory_ratio", &TThis::ContainerMemoryRatio)
        .Default();
    registrar.Parameter("container_memory_margin", &TThis::ContainerMemoryMargin)
        .Default();
    registrar.Parameter("hard", &TThis::Hard)
        .Default();
    registrar.Parameter("dump_memory_profile_on_violation", &TThis::DumpMemoryProfileOnViolation)
        .Default();
    registrar.Parameter("memory_profile_dump_timeout", &TThis::MemoryProfileDumpTimeout)
        .Default();
    registrar.Parameter("memory_profile_dump_path", &TThis::MemoryProfileDumpPath)
        .Default();
    registrar.Parameter("memory_profile_dump_filename_suffix", &TThis::MemoryProfileDumpFilenameSuffix)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TTCMallocConfigPtr TTCMallocConfig::ApplyDynamic(const TDynamicTCMallocConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->AggressiveReleaseThreshold, dynamicConfig->AggressiveReleaseThreshold);
    UpdateYsonStructField(mergedConfig->AggressiveReleaseThresholdRatio, dynamicConfig->AggressiveReleaseThresholdRatio);
    UpdateYsonStructField(mergedConfig->AggressiveReleaseSize, dynamicConfig->AggressiveReleaseSize);
    UpdateYsonStructField(mergedConfig->AggressiveReleasePeriod, dynamicConfig->AggressiveReleasePeriod);
    UpdateYsonStructField(mergedConfig->GuardedSamplingRate, dynamicConfig->GuardedSamplingRate);
    UpdateYsonStructField(mergedConfig->ProfileSamplingRate, dynamicConfig->ProfileSamplingRate);
    UpdateYsonStructField(mergedConfig->MaxPerCpuCacheSize, dynamicConfig->MaxPerCpuCacheSize);
    UpdateYsonStructField(mergedConfig->MaxTotalThreadCacheBytes, dynamicConfig->MaxTotalThreadCacheBytes);
    UpdateYsonStructField(mergedConfig->BackgroundReleaseRate, dynamicConfig->BackgroundReleaseRate);
    mergedConfig->HeapSizeLimit->ApplyDynamicInplace(dynamicConfig->HeapSizeLimit);
    mergedConfig->Postprocess();
    return mergedConfig;
}

void TTCMallocConfig::Register(TRegistrar registrar)
{
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
    registrar.Parameter("profile_sampling_rate", &TThis::ProfileSamplingRate)
        .Default(2_MB);
    registrar.Parameter("max_per_cpu_cache_size", &TThis::MaxPerCpuCacheSize)
        .Default(3_MB);
    registrar.Parameter("max_total_thread_cache_bytes", &TThis::MaxTotalThreadCacheBytes)
        .Default(24_MB);
    registrar.Parameter("background_release_rate", &TThis::BackgroundReleaseRate)
        .Default(32_MB);

    registrar.Parameter("heap_size_limit", &TThis::HeapSizeLimit)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTCMallocConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("aggressive_release_threshold", &TThis::AggressiveReleaseThreshold)
        .Default();
    registrar.Parameter("aggressive_release_threshold_ratio", &TThis::AggressiveReleaseThresholdRatio)
        .Optional();

    registrar.Parameter("aggressive_release_size", &TThis::AggressiveReleaseSize)
        .Default();
    registrar.Parameter("aggressive_release_period", &TThis::AggressiveReleasePeriod)
        .Default();
    registrar.Parameter("guarded_sampling_rate", &TThis::GuardedSamplingRate)
        .Default();
    registrar.Parameter("profile_sampling_rate", &TThis::ProfileSamplingRate)
        .Default();
    registrar.Parameter("max_per_cpu_cache_size", &TThis::MaxPerCpuCacheSize)
        .Default();
    registrar.Parameter("max_total_thread_cache_bytes", &TThis::MaxTotalThreadCacheBytes)
        .Default();
    registrar.Parameter("background_release_rate", &TThis::BackgroundReleaseRate)
        .Default();

    registrar.Parameter("heap_size_limit", &TThis::HeapSizeLimit)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc

