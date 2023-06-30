#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

class TYTAllocConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<bool> EnableAllocationProfiling;
    std::optional<double> AllocationProfilingSamplingRate;
    std::optional<std::vector<int>> SmallArenasToProfile;
    std::optional<std::vector<int>> LargeArenasToProfile;
    std::optional<int> ProfilingBacktraceDepth;
    std::optional<size_t> MinProfilingBytesUsedToReport;
    std::optional<TDuration> StockpileInterval;
    std::optional<int> StockpileThreadCount;
    std::optional<size_t> StockpileSize;
    std::optional<bool> EnableEagerMemoryRelease;
    std::optional<bool> EnableMadvisePopulate;
    std::optional<double> LargeUnreclaimableCoeff;
    std::optional<size_t> MinLargeUnreclaimableBytes;
    std::optional<size_t> MaxLargeUnreclaimableBytes;

    REGISTER_YSON_STRUCT(TYTAllocConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTAllocConfig)

class TYTProfilingConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<bool> EnableDetailedAllocationStatistics;

    REGISTER_YSON_STRUCT(TYTProfilingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYTProfilingConfig)


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
