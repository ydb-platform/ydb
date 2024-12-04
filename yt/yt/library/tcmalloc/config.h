#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTCMalloc {

////////////////////////////////////////////////////////////////////////////////

struct THeapSizeLimitConfig
    : public NYTree::TYsonStruct
{
    //! Limit program memory in terms of container memory.
    // If program heap size exceeds the limit tcmalloc is instructed to release memory to the kernel.
    std::optional<double> ContainerMemoryRatio;

    //! Similar to #ContainerMemoryRatio, but is set in terms of absolute difference from
    //! the container memory limit.
    //! For example, if ContainerMemoryLimit=200Gb and ContainerMemoryMargin=1Gb
    // then tcmalloc limit will be 199Gb.
    std::optional<double> ContainerMemoryMargin;

    //! If true tcmalloc crashes when system allocates more memory than #ContainerMemoryRatio/#ContainerMemoryMargin.
    bool Hard;

    bool DumpMemoryProfileOnViolation;
    TDuration DumpMemoryProfileTimeout;
    TString DumpMemoryProfilePath;

    REGISTER_YSON_STRUCT(THeapSizeLimitConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THeapSizeLimitConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTCMallocConfig
    : public NYTree::TYsonStruct
{
    i64 BackgroundReleaseRate;
    int MaxPerCpuCacheSize;

    //! Threshold in bytes
    i64 AggressiveReleaseThreshold;

    //! Threshold in fractions of total memory of the container
    std::optional<double> AggressiveReleaseThresholdRatio;

    i64 AggressiveReleaseSize;
    TDuration AggressiveReleasePeriod;

    //! Approximately 1/#GuardedSamplingRate of all allocations of
    //! size <= 256 KiB will be under GWP-ASAN.
    std::optional<i64> GuardedSamplingRate;

    THeapSizeLimitConfigPtr HeapSizeLimit;

    TTCMallocConfigPtr ApplyDynamic(const TTCMallocConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TTCMallocConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTCMallocConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc
