#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTCMalloc {

////////////////////////////////////////////////////////////////////////////////

struct THeapSizeLimitConfig
    : public NYTree::TYsonStruct
{
    //! Limit program memory in terms of container memory.
    //! If program heap size exceeds the limit tcmalloc is instructed to release memory to the kernel.
    std::optional<double> ContainerMemoryRatio;

    //! Similar to #ContainerMemoryRatio, but is set in terms of absolute difference from
    //! the container memory limit.
    //! For example, if ContainerMemoryLimit=200Gb and ContainerMemoryMargin=1Gb
    //! then tcmalloc limit will be 199Gb.
    std::optional<i64> ContainerMemoryMargin;

    //! If true tcmalloc crashes when system allocates more memory than #ContainerMemoryRatio/#ContainerMemoryMargin.
    bool Hard;

    bool DumpMemoryProfileOnViolation;

    TDuration MemoryProfileDumpTimeout;

    //! Filenames are as follows:
    //! $(MemoryProfileDumpPath)/$(Name)_$(MemoryProfileDumpFilenameSuffix)_$(Timestamp).$(Ext) or
    //! $(MemoryProfileDumpPath)/$(Name)_$(Timestamp).$(Ext) (if MemoryProfileDumpFilenameSuffix is missing)
    std::optional<TString> MemoryProfileDumpPath;
    std::optional<TString> MemoryProfileDumpFilenameSuffix;

    void ApplyDynamicInplace(const TDynamicHeapSizeLimitConfigPtr& dynamicConfig);
    THeapSizeLimitConfigPtr ApplyDynamic(const TDynamicHeapSizeLimitConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(THeapSizeLimitConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THeapSizeLimitConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicHeapSizeLimitConfig
    : public NYTree::TYsonStruct
{
    std::optional<double> ContainerMemoryRatio;
    std::optional<i64> ContainerMemoryMargin;

    std::optional<bool> Hard;

    std::optional<bool> DumpMemoryProfileOnViolation;

    std::optional<TDuration> MemoryProfileDumpTimeout;

    std::optional<TString> MemoryProfileDumpPath;
    std::optional<TString> MemoryProfileDumpFilenameSuffix;

    REGISTER_YSON_STRUCT(TDynamicHeapSizeLimitConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicHeapSizeLimitConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTCMallocConfig
    : public NYTree::TYsonStruct
{
    //! Threshold in bytes.
    i64 AggressiveReleaseThreshold;

    //! Threshold in fractions of total memory of the container.
    std::optional<double> AggressiveReleaseThresholdRatio;

    i64 AggressiveReleaseSize;
    TDuration AggressiveReleasePeriod;

    //! Approximately 1/#GuardedSamplingRate of all allocations of
    //! size <= 256 KiB will be under GWP-ASAN.
    std::optional<i64> GuardedSamplingRate;

    i64 ProfileSamplingRate;
    i64 MaxPerCpuCacheSize;
    i64 MaxTotalThreadCacheBytes;
    i64 BackgroundReleaseRate;

    THeapSizeLimitConfigPtr HeapSizeLimit;

    TTCMallocConfigPtr ApplyDynamic(const TDynamicTCMallocConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TTCMallocConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTCMallocConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTCMallocConfig
    : public NYTree::TYsonStruct
{
    std::optional<i64> AggressiveReleaseThreshold;

    std::optional<double> AggressiveReleaseThresholdRatio;

    std::optional<i64> AggressiveReleaseSize;
    std::optional<TDuration> AggressiveReleasePeriod;

    std::optional<i64> GuardedSamplingRate;

    std::optional<i64> ProfileSamplingRate;
    std::optional<i64> MaxPerCpuCacheSize;
    std::optional<i64> MaxTotalThreadCacheBytes;
    std::optional<i64> BackgroundReleaseRate;

    TDynamicHeapSizeLimitConfigPtr HeapSizeLimit;

    REGISTER_YSON_STRUCT(TDynamicTCMallocConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTCMallocConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc
