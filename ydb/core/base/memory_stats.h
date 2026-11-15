#pragma once

#include <ydb/core/protos/memory_stats.pb.h>

#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NKikimr::NMemory {

class TMemoryStatsAggregator {
    using TMemoryStats = NKikimrMemory::TMemoryStats;

    TMemoryStats Aggregated;
    TMap<TString, TMemoryStats> PerHost;

public:
    void Add(const TMemoryStats& memoryStats, TString host);

    TMemoryStats Aggregate();

private:
    static void AddMemoryStats(TMemoryStats& left, const TMemoryStats& right);
};

}
